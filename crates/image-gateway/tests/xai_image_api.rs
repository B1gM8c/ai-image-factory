use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gpt_image_2_gateway::{
    AppConfig, ExternalImageGatewayComponents, GeneratedImage, ImageGatewayError,
    InMemoryApiKeyStore, InMemoryUsageStore, UsageReservation, UsageSnapshot,
    admission::{InMemoryAdmissionStore, WorkLease},
    artifacts::{
        ArtifactBlobStore, GenerationResponseProjection, GenerationResultManifest,
        InMemoryArtifactBlobStore, StoredGenerationResult,
    },
    build_router_with_external_execution,
    provider_tasks::{
        ProviderProfileReadinessStore, ProviderProfileReadinessSummary, ProviderTaskStoreError,
    },
    settlement::{ExecutionSettlementStore, GenerationResultLookup, GenerationResultStatus},
};
use image::{ImageBuffer, ImageFormat, Rgba};
use serde_json::{Value, json};
use tower::ServiceExt;

struct ReadyStore;

#[async_trait]
impl ProviderProfileReadinessStore for ReadyStore {
    async fn summarize_profile_readiness(
        &self,
    ) -> Result<ProviderProfileReadinessSummary, ProviderTaskStoreError> {
        Ok(ProviderProfileReadinessSummary::default())
    }
}

struct ImmediateSettlement {
    storage_identity: String,
    result: StoredGenerationResult,
}

#[async_trait]
impl ExecutionSettlementStore for ImmediateSettlement {
    fn artifact_storage_identity(&self) -> String {
        self.storage_identity.clone()
    }

    async fn succeed(
        &self,
        _lease: &WorkLease,
        _reservation: &UsageReservation,
        _result: &GenerationResultManifest,
    ) -> Result<UsageSnapshot, ImageGatewayError> {
        Err(ImageGatewayError::internal("unused test settlement path"))
    }

    async fn fail(
        &self,
        _lease: &WorkLease,
        _reservation: &UsageReservation,
        _error_code: &'static str,
    ) -> Result<(), ImageGatewayError> {
        Err(ImageGatewayError::internal("unused test settlement path"))
    }

    async fn load_generation_result(
        &self,
        _job_id: uuid::Uuid,
    ) -> Result<GenerationResultLookup, ImageGatewayError> {
        Ok(GenerationResultLookup::Available(self.result.clone()))
    }

    async fn generation_status(
        &self,
        _job_id: uuid::Uuid,
    ) -> Result<GenerationResultStatus, ImageGatewayError> {
        Ok(GenerationResultStatus::Succeeded(self.result.clone()))
    }
}

#[tokio::test]
async fn grok_image_generation_uses_xai_shape_and_returns_base64_only() {
    let png = tiny_png();
    let blobs = Arc::new(InMemoryArtifactBlobStore::default());
    let settlement = Arc::new(ImmediateSettlement {
        storage_identity: blobs.storage_identity(),
        result: StoredGenerationResult {
            projection: GenerationResponseProjection {
                api_profile: "xai-images-v1".to_owned(),
                operation: "generation".to_owned(),
                response_schema: "xai.images.response.v1".to_owned(),
                created_at_seconds: 1,
                output_format: "png".to_owned(),
                quality: "auto".to_owned(),
                size: "1:1".to_owned(),
                background: "auto".to_owned(),
                stream: false,
                usage: UsageSnapshot {
                    limit_5h: 10,
                    remaining_5h: 9,
                    limit_7d: 50,
                    remaining_7d: 49,
                },
            },
            images: vec![GeneratedImage { bytes: png.clone() }],
        },
    });
    let app = build_router_with_external_execution(
        config(),
        ExternalImageGatewayComponents {
            usage_store: Arc::new(InMemoryUsageStore::default()),
            api_key_store: Arc::new(InMemoryApiKeyStore::default()),
            admission_store: Arc::new(InMemoryAdmissionStore::default()),
            settlement_store: settlement,
            input_blob_store: blobs,
            provider_readiness_store: Arc::new(ReadyStore),
        },
    )
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/images/generations")
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "model": "grok-imagine-image-quality",
                        "prompt": "a precise monochrome icon",
                        "n": 1,
                        "resolution": "1k",
                        "response_format": "b64_json"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    let encoded = body["data"][0]["b64_json"].as_str().unwrap();
    assert_eq!(STANDARD.decode(encoded).unwrap(), png);
    assert_eq!(body["data"][0]["mime_type"], "image/png");
    assert!(body["data"][0].get("url").is_none());
    assert!(body["data"][0].get("file_output").is_none());
}

fn config() -> AppConfig {
    AppConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: Some("test-token".to_owned()),
        admin_token: Some("admin-token".to_owned()),
        legacy_admin_auth_enabled: true,
        database_url: None,
        generation_admission_contract: Default::default(),
        enable_xai_video_api: false,
        five_hour_image_limit: 10,
        seven_day_image_limit: 50,
        five_hour_video_second_limit: u32::MAX,
        seven_day_video_second_limit: u32::MAX,
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

fn tiny_png() -> Vec<u8> {
    let image = ImageBuffer::from_pixel(1, 1, Rgba([0u8, 0, 0, 255]));
    let mut cursor = std::io::Cursor::new(Vec::new());
    image.write_to(&mut cursor, ImageFormat::Png).unwrap();
    cursor.into_inner()
}
