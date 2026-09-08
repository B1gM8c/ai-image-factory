use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    response::IntoResponse,
};
use gpt_image_2_gateway::{
    ApiKeyPermissionMode, ApiKeyPermissions, ApiKeyStore, AppConfig, ExternalControlPlaneServices,
    ExternalImageGatewayComponents, ImageGatewayError, InMemoryApiKeyStore, InMemoryUsageStore,
    PostgresExecutionSettlementStore, PostgresProviderTaskStore,
    admission::InMemoryAdmissionStore,
    artifacts::InMemoryArtifactBlobStore,
    build_router_with_external_execution_and_services,
    database::connect_test_pool_with_search_path,
    input_blobs::{
        InputBlobDeleteError, InputBlobKey, InputBlobReadError, InputBlobRef, InputBlobStore,
        InputBlobWriteError,
    },
    media_segments::*,
};
use image::{DynamicImage, ImageFormat};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, PgPool};
use std::{
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tower::ServiceExt;
use uuid::Uuid;

struct Harness {
    schema: String,
    pool: PgPool,
    app: Router,
    store: Arc<PostgresSegmentStore>,
    blobs: Arc<InMemoryArtifactBlobStore>,
    keys: Arc<InMemoryApiKeyStore>,
}

impl Harness {
    async fn new() -> Option<Self> {
        Self::with_enabled(true).await
    }

    async fn with_enabled(enabled: bool) -> Option<Self> {
        let url = match std::env::var("TEST_DATABASE_URL")
            .or_else(|_| std::env::var("GATEWAY_TEST_DATABASE_URL"))
        {
            Ok(url) => url,
            Err(_) => {
                assert!(
                    std::env::var_os("CI").is_none(),
                    "TEST_DATABASE_URL required in CI"
                );
                return None;
            }
        };
        let schema = format!("bbox_http_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 5, &schema)
            .await
            .unwrap();
        let database: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(database.contains("test"));
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&pool)
            .await
            .unwrap();
        // The sidecar works without any generation jobs, billing, or provider-profile tables.
        sqlx::raw_sql(include_str!("../migrations/0129_media_segments.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(
            "../migrations/0130_media_segment_source_lifecycle.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let store = Arc::new(PostgresSegmentStore::new(pool.clone()));
        let blobs = Arc::new(InMemoryArtifactBlobStore::default());
        let keys = Arc::new(InMemoryApiKeyStore::default());
        let service = Arc::new(MediaSegmentsService::new(
            store.clone(),
            blobs.clone(),
            AnalyzerConfig::default(),
        ));
        let app = build_router_with_external_execution_and_services(
            config(),
            ExternalImageGatewayComponents {
                usage_store: Arc::new(InMemoryUsageStore::default()),
                api_key_store: keys.clone(),
                admission_store: Arc::new(InMemoryAdmissionStore::default()),
                settlement_store: Arc::new(PostgresExecutionSettlementStore::new(
                    pool.clone(),
                    blobs.clone(),
                )),
                input_blob_store: blobs.clone(),
                provider_readiness_store: Arc::new(PostgresProviderTaskStore::new(pool.clone())),
            },
            ExternalControlPlaneServices {
                media_segments_service: enabled.then_some(service),
                ..Default::default()
            },
        )
        .unwrap();
        Some(Self {
            schema,
            pool,
            app,
            store,
            blobs,
            keys,
        })
    }

    async fn cleanup(self) {
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .unwrap();
        self.pool.close().await;
    }

    async fn upload(&self, image: &[u8]) -> (StatusCode, Value) {
        let mut multipart = b"--bbox-boundary\r\nContent-Disposition: form-data; name=\"image\"; filename=\"image.png\"\r\nContent-Type: image/png\r\n\r\n".to_vec();
        multipart.extend(image);
        multipart.extend(b"\r\n--bbox-boundary--\r\n");
        self.call(
            "POST",
            "/v1/media/assets",
            "test-token",
            "multipart/form-data; boundary=bbox-boundary",
            multipart,
        )
        .await
    }

    async fn call(
        &self,
        method: &str,
        path: &str,
        token: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", content_type)
            .body(Body::from(body))
            .unwrap();
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        assert!(
            response
                .headers()
                .get("cache-control")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("no-store")
        );
        if status == StatusCode::ACCEPTED {
            assert_eq!(response.headers()["retry-after"], "2");
        }
        let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    async fn segment(&self, asset_id: &str, cached_only: bool) -> (StatusCode, Value) {
        self.call(
            "POST",
            "/v1/media/segments",
            "test-token",
            "application/json",
            serde_json::to_vec(&json!({"asset_id":asset_id,"cached_only":cached_only})).unwrap(),
        )
        .await
    }
}

fn config() -> AppConfig {
    AppConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: Some("test-token".into()),
        admin_token: None,
        legacy_admin_auth_enabled: false,
        database_url: None,
        generation_admission_contract: Default::default(),
        enable_xai_video_api: false,
        five_hour_image_limit: 10,
        seven_day_image_limit: 50,
        five_hour_video_second_limit: 100,
        seven_day_video_second_limit: 500,
        max_concurrent_jobs: 1,
        max_queue_size: 4,
        max_concurrent_jobs_per_tenant: 1,
        max_queue_size_per_tenant: 4,
        queue_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        readiness_timeout: Duration::from_millis(500),
        readiness_stall_threshold: Duration::from_secs(60),
        max_upload_bytes: 32 * 1024 * 1024,
        proxy: Default::default(),
        codex_home: None,
        cleanup_codex_outputs: false,
    }
}

fn png() -> Vec<u8> {
    let mut out = Cursor::new(Vec::new());
    DynamicImage::new_rgb8(100, 80)
        .write_to(&mut out, ImageFormat::Png)
        .unwrap();
    out.into_inner()
}

#[test]
fn published_blog_fixture_round_trips_the_public_contract() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/contracts/media-segmentation-v1.json"
    ))
    .unwrap();
    let result: Segmentation = serde_json::from_value(fixture.clone()).unwrap();
    assert_eq!(result.status, SegmentStatus::Completed);
    assert_eq!(serde_json::to_value(result).unwrap(), fixture);
}

#[tokio::test]
async fn absent_sidecar_is_default_off_without_enqueuing_work() {
    let Some(h) = Harness::with_enabled(false).await else {
        return;
    };
    let (status, capabilities) = h
        .call(
            "GET",
            "/v1/media/capabilities",
            "test-token",
            "application/json",
            vec![],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        capabilities,
        json!({"supports_bbox_sidecar":false,"supports_mask_sidecar":false,"supports_terminal_source_release":false,"supports_analyzer_key_pin":false})
    );
    assert_eq!(h.upload(&png()).await.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        h.segment("img_0123456789abcdef0123456789abcdef", false)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        h.call(
            "GET",
            "/v1/media/segments/seg_0123456789abcdef0123456789abcdef",
            "test-token",
            "application/json",
            vec![]
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_segment_results")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    h.cleanup().await;
}

struct FakeAnalyzer {
    calls: AtomicUsize,
    fail: bool,
}

#[async_trait]
impl BboxAnalyzer for FakeAnalyzer {
    async fn analyze(
        &self,
        _bytes: &[u8],
        _image: &ImageSize,
    ) -> Result<Analysis, ImageGatewayError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(ImageGatewayError::service_unavailable(
                "private provider diagnostic",
            ));
        }
        Ok(Analysis { candidate: serde_json::from_value(json!({"groups":[{
            "parent_index":null,"名称":"橘猫","类别":"主体","bbox_xyxy":[10,10,80,70],"confidence":0.9,
            "segments":[{"名称":"猫头","bbox_xyxy":[9,12,60,40],"confidence":0.8}]
        }]})).unwrap(), timings: SegmentTimings::default() })
    }
}

#[tokio::test]
async fn api_register_queue_poll_cache_and_failure_isolation() {
    let Some(h) = Harness::new().await else {
        return;
    };
    let (status, asset) = h.upload(&png()).await;
    assert_eq!(status, StatusCode::OK, "{asset}");
    assert_eq!(asset["image"]["width"], 100);
    let asset_id = asset["asset_id"].as_str().unwrap();
    assert_eq!(h.upload(&png()).await.1, asset);
    let (status, miss) = h.segment(asset_id, true).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(miss["error"]["code"], "segments_not_cached");
    let (status, queued) = h.segment(asset_id, false).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{queued}");
    assert_eq!(h.segment(asset_id, false).await.1, queued);
    let pinned_body = serde_json::to_vec(&json!({"asset_id":asset_id,"cached_only":false,"expected_analyzer_key":analyzer_key(&AnalyzerConfig::default())})).unwrap();
    assert_eq!(
        h.call(
            "POST",
            "/v1/media/segments",
            "test-token",
            "application/json",
            pinned_body
        )
        .await
        .1,
        queued,
        "pin must not change the preexisting cache identity"
    );
    let analyzer = Arc::new(FakeAnalyzer {
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let worker = SegmentWorker::new(
        h.store.clone(),
        h.blobs.clone(),
        analyzer.clone(),
        &AnalyzerConfig::default(),
    );
    assert!(worker.run_once().await.unwrap());
    assert!(!worker.run_once().await.unwrap());
    let (status, done) = h.segment(asset_id, true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(done["status"], "completed");
    assert_eq!(done["groups"][0]["bbox_xyxy"], json!([9, 10, 80, 70]));
    let path = format!("/v1/media/segments/{}", done["id"].as_str().unwrap());
    assert_eq!(
        h.call("GET", &path, "test-token", "application/json", vec![])
            .await
            .1,
        done
    );
    let mut samples = Vec::new();
    for _ in 0..25 {
        let start = Instant::now();
        assert_eq!(h.segment(asset_id, true).await.1, done);
        samples.push(start.elapsed().as_micros());
    }
    samples.sort();
    eprintln!(
        "cached HTTP in-process n=25 P50={}us P95={}us max={}us (includes auth + 2 PostgreSQL reads)",
        samples[12], samples[23], samples[24]
    );
    assert_eq!(analyzer.calls.load(Ordering::SeqCst), 1);
    // A separate model/config creates its own sidecar result, but never changes asset state.
    let failed_config = AnalyzerConfig {
        revision: "failure-case".into(),
        ..Default::default()
    };
    let failed_service =
        MediaSegmentsService::new(h.store.clone(), h.blobs.clone(), failed_config.clone());
    let scope = MediaScope {
        tenant_id: "tenant_default".into(),
        project_id: "proj_default".into(),
        owner_id: "".into(),
    };
    let request: SegmentRequest =
        serde_json::from_value(json!({"asset_id":asset_id,"cached_only":false})).unwrap();
    let failure = failed_service
        .request(&scope, request.clone())
        .await
        .unwrap();
    let failing = Arc::new(FakeAnalyzer {
        calls: AtomicUsize::new(0),
        fail: true,
    });
    let failed_worker = SegmentWorker::new(
        h.store.clone(),
        h.blobs.clone(),
        failing.clone(),
        &failed_config,
    );
    assert!(failed_worker.run_once().await.unwrap());
    let terminal = failed_service.get(&scope, &failure.id).await.unwrap();
    assert_eq!(terminal.status, SegmentStatus::Failed);
    assert!(
        !serde_json::to_string(&terminal)
            .unwrap()
            .contains("private provider diagnostic")
    );
    assert_eq!(
        failed_service.request(&scope, request).await.unwrap(),
        terminal
    );
    assert!(!failed_worker.run_once().await.unwrap());
    assert_eq!(failing.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.upload(&png()).await.1, asset);
    assert_eq!(h.segment(asset_id, true).await.1, done);
    h.cleanup().await;
}

#[tokio::test]
async fn api_scope_and_permissions_are_enforced() {
    let Some(h) = Harness::new().await else {
        return;
    };
    let asset = h.upload(&png()).await.1;
    let asset_id = asset["asset_id"].as_str().unwrap();
    let queued = h.segment(asset_id, false).await.1;
    let project = h.keys.create_project("Other project").await.unwrap();
    let other = h
        .keys
        .create_service_account(
            &project.id,
            "Other",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .unwrap();
    let readonly = h
        .keys
        .create_service_account(
            "proj_default",
            "Read",
            ApiKeyPermissionMode::ReadOnly,
            ApiKeyPermissions::default(),
        )
        .await
        .unwrap();
    let write_body = serde_json::to_vec(&json!({"asset_id":asset_id,"cached_only":false})).unwrap();
    let read_body = serde_json::to_vec(&json!({"asset_id":asset_id,"cached_only":true})).unwrap();
    assert_eq!(
        h.call(
            "POST",
            "/v1/media/segments",
            &other.api_key.value,
            "application/json",
            read_body.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let path = format!("/v1/media/segments/{}", queued["id"].as_str().unwrap());
    assert_eq!(
        h.call(
            "GET",
            &path,
            &other.api_key.value,
            "application/json",
            vec![]
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        h.call(
            "POST",
            "/v1/media/segments",
            &readonly.api_key.value,
            "application/json",
            write_body
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        h.call(
            "POST",
            "/v1/media/segments",
            &readonly.api_key.value,
            "application/json",
            read_body
        )
        .await
        .0,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        h.call(
            "GET",
            &path,
            &readonly.api_key.value,
            "application/json",
            vec![]
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(h.upload(b"corrupt image").await.0, StatusCode::BAD_REQUEST);
    let (status, error) = h
        .call(
            "POST",
            "/v1/media/assets",
            "test-token",
            "multipart/form-data",
            vec![],
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["error"]["code"], "invalid_image_upload");
    let mut too_large = b"--bbox-boundary\r\nContent-Disposition: form-data; name=\"image\"; filename=\"image.png\"\r\n\r\n".to_vec();
    too_large.resize(MAX_ASSET_BYTES + 2 * 1024 * 1024, b'x');
    assert_eq!(
        h.call(
            "POST",
            "/v1/media/assets",
            "test-token",
            "multipart/form-data; boundary=bbox-boundary",
            too_large
        )
        .await
        .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    h.cleanup().await;
}

#[tokio::test]
async fn readiness_requires_authorized_fresh_matching_worker_without_enqueuing_analysis() {
    let Some(h) = Harness::new().await else {
        return;
    };
    let path = "/v1/media/readiness";
    assert_eq!(
        h.call("GET", path, "invalid-token", "application/json", vec![])
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let (status, missing) = h
        .call("GET", path, "test-token", "application/json", vec![])
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(missing["reason"], "worker_heartbeat_missing");
    let current = analyzer_key(&AnalyzerConfig::default());
    assert_eq!(missing["analyzer_key"], current);
    h.store
        .record_worker_heartbeat(&"a".repeat(64), true)
        .await
        .unwrap();
    assert_eq!(
        h.call("GET", path, "test-token", "application/json", vec![])
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    h.store
        .record_worker_heartbeat(&current, true)
        .await
        .unwrap();
    let (status, fresh) = h
        .call("GET", path, "test-token", "application/json", vec![])
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fresh["status"], "ready");
    assert_eq!(fresh["source_release_enabled"], true);
    assert_eq!(fresh["heartbeat_ttl_ms"], 150_000);
    let readonly = h
        .keys
        .create_service_account(
            "proj_default",
            "Read",
            ApiKeyPermissionMode::ReadOnly,
            ApiKeyPermissions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        h.call(
            "GET",
            path,
            &readonly.api_key.value,
            "application/json",
            vec![]
        )
        .await
        .0,
        StatusCode::OK
    );
    let other_analyzer = "b".repeat(64);
    h.store
        .record_worker_heartbeat(&other_analyzer, false)
        .await
        .unwrap();
    let (status, other_mode) = h
        .call("GET", path, "test-token", "application/json", vec![])
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(other_mode["reason"], "worker_configuration_mismatch");
    sqlx::query(
        "UPDATE media_segment_worker_heartbeats SET observed_at_ms=$1 WHERE analyzer_key=$2",
    )
    .bind(now_ms() - 150_001)
    .bind(&other_analyzer)
    .execute(&h.pool)
    .await
    .unwrap();
    assert_eq!(
        h.call("GET", path, "test-token", "application/json", vec![])
            .await
            .0,
        StatusCode::OK
    );
    h.store
        .record_worker_heartbeat(&current, false)
        .await
        .unwrap();
    let (status, mixed) = h
        .call("GET", path, "test-token", "application/json", vec![])
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(mixed["reason"], "worker_configuration_mismatch");
    assert_eq!(mixed["source_release_enabled"], serde_json::Value::Null);
    // A final heartbeat from the disabled worker cannot hide a still-live
    // source-deleting worker. Both modes must age out independently.
    h.store
        .record_worker_heartbeat(&current, false)
        .await
        .unwrap();
    assert_eq!(
        h.call("GET", path, "test-token", "application/json", vec![])
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    sqlx::query(
        "UPDATE media_segment_worker_heartbeats SET observed_at_ms = $1 WHERE analyzer_key = $2",
    )
    .bind(now_ms() - 150_001)
    .bind(&current)
    .execute(&h.pool)
    .await
    .unwrap();
    let (status, stale) = h
        .call("GET", path, "test-token", "application/json", vec![])
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(stale["reason"], "worker_heartbeat_stale");
    let (_, capabilities) = h
        .call(
            "GET",
            "/v1/media/capabilities",
            "test-token",
            "application/json",
            vec![],
        )
        .await;
    assert_eq!(
        capabilities,
        json!({"supports_bbox_sidecar":true,"supports_mask_sidecar":false,"supports_terminal_source_release":true,"supports_analyzer_key_pin":true})
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_segment_results")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    h.cleanup().await;
}

#[tokio::test]
async fn analyzer_pin_recovers_old_pending_and_terminal_result_without_new_paid_work() {
    let Some(h) = Harness::new().await else {
        return;
    };
    let scope = MediaScope {
        tenant_id: "tenant_default".into(),
        project_id: "proj_default".into(),
        owner_id: "".into(),
    };
    let old = AnalyzerConfig {
        revision: "before-deploy".into(),
        ..Default::default()
    };
    let old_key = analyzer_key(&old);
    let old_service = MediaSegmentsService::new(h.store.clone(), h.blobs.clone(), old.clone());
    let registered = old_service.register_asset(&scope, png()).await.unwrap();
    let request: SegmentRequest = serde_json::from_value(
        json!({"asset_id":registered.asset_id,"cached_only":false,"expected_analyzer_key":old_key}),
    )
    .unwrap();
    let pending = old_service.request(&scope, request).await.unwrap();
    let body=serde_json::to_vec(&json!({"asset_id":registered.asset_id,"cached_only":false,"expected_analyzer_key":old_key})).unwrap();
    let (status, recovered) = h
        .call(
            "POST",
            "/v1/media/segments",
            "test-token",
            "application/json",
            body.clone(),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(recovered["id"], pending.id);
    let worker = SegmentWorker::new(
        h.store.clone(),
        h.blobs.clone(),
        Arc::new(FakeAnalyzer {
            calls: AtomicUsize::new(0),
            fail: false,
        }),
        &old,
    )
    .with_terminal_source_release(true);
    worker.run_once().await.unwrap();
    worker.maintain().await.unwrap();
    let (status, completed) = h
        .call(
            "POST",
            "/v1/media/segments",
            "test-token",
            "application/json",
            body,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["status"], "completed");
    assert_eq!(completed["id"], pending.id);
    let mismatch=serde_json::to_vec(&json!({"asset_id":registered.asset_id,"cached_only":false,"expected_analyzer_key":"b".repeat(64)})).unwrap();
    let (status, error) = h
        .call(
            "POST",
            "/v1/media/segments",
            "test-token",
            "application/json",
            mismatch,
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error["error"]["code"], "segmentation_analyzer_changed");
    for bad in [
        String::new(),
        "A".repeat(64),
        "g".repeat(64),
        "a".repeat(63),
    ] {
        let body =
            serde_json::to_vec(&json!({"asset_id":"invalid","expected_analyzer_key":bad})).unwrap();
        let (status, error) = h
            .call(
                "POST",
                "/v1/media/segments",
                "test-token",
                "application/json",
                body,
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(error["error"]["code"], "invalid_analyzer_key");
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_segment_results")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    h.cleanup().await;
}

#[tokio::test]
async fn terminal_sources_allow_65_serial_images_without_repeating_analysis_or_retaining_pixels() {
    let Some(h) = Harness::new().await else {
        return;
    };
    let analyzer = Arc::new(FakeAnalyzer {
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let worker = SegmentWorker::new(
        h.store.clone(),
        h.blobs.clone(),
        analyzer.clone(),
        &AnalyzerConfig::default(),
    )
    .with_terminal_source_release(true);
    let mut first = None;
    for number in 0..65 {
        let mut bytes = png();
        bytes.extend_from_slice(format!("fixture-{number}").as_bytes());
        let (status, registered) = h.upload(&bytes).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "serial asset {number}: {registered}"
        );
        let asset_id = registered["asset_id"].as_str().unwrap();
        let (status, queued) = h.segment(asset_id, false).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(worker.run_once().await.unwrap());
        worker.maintain().await.unwrap();
        let (status, completed) = h.segment(asset_id, true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(completed["status"], "completed");
        assert_eq!(completed["id"], queued["id"]);
        let assets: (i64, i64, i64) = sqlx::query_as("SELECT COUNT(*), COUNT(*) FILTER (WHERE source_state <> 'released'), COALESCE(SUM(byte_size) FILTER (WHERE source_state <> 'released'),0)::BIGINT FROM media_segment_assets")
            .fetch_one(&h.pool).await.unwrap();
        assert_eq!(assets, (number + 1, 0, 0));
        if number == 0 {
            first = Some((bytes, registered, completed));
        }
    }
    let (bytes, registered, completed) = first.unwrap();
    let (status, duplicate) = h.upload(&bytes).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        duplicate, registered,
        "same config replay must not rehydrate released bytes"
    );
    assert_eq!(
        h.segment(registered["asset_id"].as_str().unwrap(), false)
            .await
            .1,
        completed
    );
    assert!(!worker.run_once().await.unwrap());
    assert_eq!(analyzer.calls.load(Ordering::SeqCst), 65);
    let sources: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM media_segment_assets WHERE source_state <> 'released'",
    )
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(sources, 0);
    h.cleanup().await;
}

#[tokio::test]
async fn unknown_commit_timeout_preserves_pixels_for_late_registration_and_replay() {
    use sha2::{Digest, Sha256};

    let Some(h) = Harness::new().await else {
        return;
    };
    // This intentionally uses the ordinary test pool, not the production pool's
    // shorter statement deadline: the COMMIT must outlive the 5s application
    // deadline to reproduce an unknown outcome without a mock database.
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION delay_asset_commit() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_sleep(6.5);
            RETURN NEW;
        END;
        $$;
        CREATE CONSTRAINT TRIGGER delay_asset_commit
            AFTER INSERT ON media_segment_assets
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            EXECUTE FUNCTION delay_asset_commit();
        "#,
    )
    .execute(&h.pool)
    .await
    .unwrap();
    let scope = MediaScope {
        tenant_id: "tenant_default".into(),
        project_id: "proj_default".into(),
        owner_id: "".into(),
    };
    let bytes = png();
    let digest = hex::encode(Sha256::digest(&bytes));
    let started = Instant::now();
    let (status, error) = h.upload(&bytes).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{error}");
    assert!(started.elapsed() >= Duration::from_secs(5));
    assert!(
        h.store.find_asset(&scope, &digest).await.unwrap().is_none(),
        "a separate connection still cannot see the pending COMMIT"
    );

    let committed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(asset) = h.store.find_asset(&scope, &digest).await.unwrap() {
                break asset;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the original COMMIT must eventually become visible");
    assert_eq!(
        h.blobs.get(&committed.blob).await.unwrap(),
        bytes,
        "an unknown COMMIT must not delete pixels referenced by its late commit"
    );
    let (status, replay) = h.upload(&bytes).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay, serde_json::to_value(committed.response()).unwrap());
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_segment_assets")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(rows, 1, "replay must retain the original asset identity");
    h.cleanup().await;
}

struct FaultyDeletionBlobs {
    inner: Arc<InMemoryArtifactBlobStore>,
    fail_delete: AtomicBool,
    deleted_sessions: std::sync::Mutex<Vec<Uuid>>,
}

#[async_trait]
impl InputBlobStore for FaultyDeletionBlobs {
    async fn put(
        &self,
        key: InputBlobKey,
        bytes: &[u8],
    ) -> Result<InputBlobRef, InputBlobWriteError> {
        self.inner.put(key, bytes).await
    }
    async fn get(&self, blob: &InputBlobRef) -> Result<Vec<u8>, InputBlobReadError> {
        self.inner.get(blob).await
    }
    async fn delete(&self, blob: &InputBlobRef) -> Result<(), InputBlobDeleteError> {
        self.inner.delete(blob).await
    }
    async fn delete_session(&self, session: Uuid) -> Result<(), InputBlobDeleteError> {
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err(InputBlobDeleteError::Unavailable);
        }
        self.deleted_sessions.lock().unwrap().push(session);
        self.inner.delete_session(session).await
    }
}

#[tokio::test]
async fn deletion_failure_cache_reads_restore_and_stale_confirmation_are_fenced() {
    let Some(h) = Harness::new().await else {
        return;
    };
    let scope = MediaScope {
        tenant_id: "tenant_default".into(),
        project_id: "proj_default".into(),
        owner_id: "".into(),
    };
    let bytes = png();
    let registered = h.upload(&bytes).await.1;
    let asset_id = registered["asset_id"].as_str().unwrap();
    let id = Uuid::parse_str(asset_id.strip_prefix("img_").unwrap()).unwrap();
    h.segment(asset_id, false).await;
    let blobs = Arc::new(FaultyDeletionBlobs {
        inner: h.blobs.clone(),
        fail_delete: AtomicBool::new(true),
        deleted_sessions: Default::default(),
    });
    let analyzer = Arc::new(FakeAnalyzer {
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let worker = SegmentWorker::new(
        h.store.clone(),
        blobs.clone(),
        analyzer.clone(),
        &AnalyzerConfig::default(),
    )
    .with_terminal_source_release(true);
    worker.run_once().await.unwrap();
    let original_result = h.segment(asset_id, true).await.1;
    assert!(worker.maintain().await.is_err());
    let releasing = h.store.get_asset(&scope, id).await.unwrap().unwrap();
    assert_eq!(releasing.source_state, SourceState::Releasing);
    assert_eq!(h.blobs.get(&releasing.blob).await.unwrap(), bytes);
    assert_eq!(h.segment(asset_id, false).await.1, original_result);
    assert_eq!(h.upload(&bytes).await.1, registered);

    let revised = AnalyzerConfig {
        revision: "restored-model-revision".into(),
        ..Default::default()
    };
    let service = MediaSegmentsService::new(h.store.clone(), blobs.clone(), revised.clone());
    let request: SegmentRequest =
        serde_json::from_value(json!({"asset_id":asset_id,"cached_only":false})).unwrap();
    assert_eq!(
        error_code(service.request(&scope, request.clone()).await.unwrap_err()).await,
        "asset_source_releasing"
    );
    assert_eq!(
        error_code(
            service
                .register_asset(&scope, bytes.clone())
                .await
                .unwrap_err()
        )
        .await,
        "asset_source_releasing"
    );
    blobs.fail_delete.store(false, Ordering::SeqCst);
    // Simulate a process crash after deletion but before its database acknowledgement.
    blobs
        .delete_session(releasing.blob.key.admission_session_id)
        .await
        .unwrap();
    assert_eq!(
        h.store
            .get_asset(&scope, id)
            .await
            .unwrap()
            .unwrap()
            .source_state,
        SourceState::Releasing
    );
    worker.maintain().await.unwrap();
    assert!(h.blobs.get(&releasing.blob).await.is_err());
    assert_eq!(
        error_code(service.request(&scope, request.clone()).await.unwrap_err()).await,
        "asset_source_released"
    );
    assert_eq!(
        service
            .register_asset(&scope, bytes.clone())
            .await
            .unwrap()
            .asset_id,
        asset_id
    );
    let restored = h.store.get_asset(&scope, id).await.unwrap().unwrap();
    assert_eq!(restored.source_state, SourceState::Retained);
    worker.maintain().await.unwrap();
    assert_eq!(
        h.store
            .get_asset(&scope, id)
            .await
            .unwrap()
            .unwrap()
            .source_state,
        SourceState::Retained,
        "old terminal result must not release a newly restored input before its new enqueue"
    );
    assert_eq!(restored.expires_at_ms, releasing.expires_at_ms);
    assert_ne!(
        restored.blob.key.admission_session_id,
        releasing.blob.key.admission_session_id
    );
    assert_eq!(h.blobs.get(&restored.blob).await.unwrap(), bytes);
    // Simulate a second cleaner resuming after the first cleaner completed and
    // a client restored new bytes: old-session deletion and CAS stay harmless.
    blobs
        .delete_session(releasing.blob.key.admission_session_id)
        .await
        .unwrap();
    assert!(!h.store.finish_source_release(&releasing).await.unwrap());
    assert_eq!(h.blobs.get(&restored.blob).await.unwrap(), bytes);
    let pending = service.request(&scope, request).await.unwrap();
    assert_ne!(pending.id, original_result["id"]);
    assert_eq!(h.segment(asset_id, true).await.1, original_result);
    let revised_worker =
        SegmentWorker::new(h.store.clone(), blobs.clone(), analyzer.clone(), &revised)
            .with_terminal_source_release(true);
    revised_worker.run_once().await.unwrap();
    revised_worker.maintain().await.unwrap();
    assert!(h.blobs.get(&restored.blob).await.is_err());
    assert_eq!(
        blobs.deleted_sessions.lock().unwrap().last(),
        Some(&restored.blob.key.admission_session_id)
    );
    assert_eq!(analyzer.calls.load(Ordering::SeqCst), 2);
    h.cleanup().await;
}

#[tokio::test]
#[ignore = "requires explicit local Codex credentials and live model execution"]
async fn live_codex_asset_to_segments_and_cached_replay() {
    let Some(h) = Harness::new().await else {
        panic!("test database required");
    };
    let image_path = std::env::var("BBOX_SMOKE_IMAGE").expect("BBOX_SMOKE_IMAGE");
    let bytes = std::fs::read(image_path).unwrap();
    let (status, asset) = h.upload(&bytes).await;
    assert_eq!(status, StatusCode::OK, "{asset}");
    let asset_id = asset["asset_id"].as_str().unwrap();
    assert_eq!(h.segment(asset_id, false).await.0, StatusCode::ACCEPTED);
    let analyzer = Arc::new(
        CodexBboxAnalyzer::new(
            std::env::var("GATEWAY_BBOX_CODEX_BIN").unwrap().into(),
            std::env::var("GATEWAY_BBOX_CODEX_HOME").unwrap().into(),
            AnalyzerConfig::default(),
        )
        .unwrap(),
    );
    let worker = SegmentWorker::new(
        h.store.clone(),
        h.blobs.clone(),
        analyzer,
        &AnalyzerConfig::default(),
    );
    let start = Instant::now();
    assert!(worker.run_once().await.unwrap());
    let elapsed = start.elapsed();
    let (status, result) = h.segment(asset_id, true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["status"], "completed", "{result}");
    assert!(!result["groups"].as_array().unwrap().is_empty());
    let start = Instant::now();
    assert_eq!(h.segment(asset_id, true).await.1, result);
    eprintln!(
        "live bbox worker={}ms cached={}us result={result}",
        elapsed.as_millis(),
        start.elapsed().as_micros()
    );
    h.cleanup().await;
}

async fn error_code(error: ImageGatewayError) -> String {
    let bytes = to_bytes(error.into_response().into_body(), 16 * 1024)
        .await
        .unwrap();
    serde_json::from_slice::<Value>(&bytes).unwrap()["error"]["code"]
        .as_str()
        .unwrap()
        .to_owned()
}
