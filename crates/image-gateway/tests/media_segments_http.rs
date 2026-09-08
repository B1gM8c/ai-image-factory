use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use gpt_image_2_gateway::{
    ApiKeyPermissionMode, ApiKeyPermissions, ApiKeyStore, AppConfig, ExternalControlPlaneServices,
    ExternalImageGatewayComponents, ImageGatewayError, InMemoryApiKeyStore, InMemoryUsageStore,
    PostgresExecutionSettlementStore, PostgresProviderTaskStore, admission::InMemoryAdmissionStore,
    artifacts::InMemoryArtifactBlobStore, build_router_with_external_execution_and_services,
    database::connect_test_pool_with_search_path, media_segments::*,
};
use image::{DynamicImage, ImageFormat};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, PgPool};
use std::{
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
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
        json!({"supports_bbox_sidecar":false,"supports_mask_sidecar":false})
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
