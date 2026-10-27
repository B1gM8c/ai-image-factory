use super::*;
use gpt_image_2_gateway::admission::{AdmissionClaim, ClaimAdmission, idempotency_key_digest};

const PROFILE: &str = "dreamina-cli-videos-v1";
const LOOKUP: &str = "/v1/dreamina/videos/by-idempotency-key";
const KEY: &str = "original-lost-video-response";

#[tokio::test]
async fn video_lookup_recovers_lost_admission_response_without_resubmitting_or_charging()
-> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = Box::pin(run_lookup(&database.pool)).await;
    combine(result, database.cleanup().await)
}

async fn run_lookup(pool: &PgPool) -> TestResult {
    let root = TempDir::new().map_err(debug_error)?;
    let home = root.path().join("home");
    fs::create_dir(&home).map_err(debug_error)?;
    fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
    let profile = provision_dreamina_video_execution_profile(
        pool,
        &DreaminaExecutionProfileProvisioning {
            profile_key: "lookup-fixture".into(),
            credential_pool_key: "lookup-fixture".into(),
            provider_account_key: "lookup-fixture".into(),
            credential_ref: "private:lookup-fixture".into(),
            credential_revision: 1,
            credential_auth_sha256: "a".repeat(64),
            max_concurrency: 1,
        },
    )
    .await
    .map_err(debug_error)?;
    let provider = PostgresProviderTaskStore::new(pool.clone());
    for role in [ProviderRuntimeRole::Submit, ProviderRuntimeRole::Poll] {
        provider
            .register_runtime(
                &ProviderRuntimeRegistration {
                    runtime_id: Uuid::new_v4(),
                    execution_profile_id: profile.execution_profile_id,
                    role,
                    runtime_owner: format!("lookup-{role:?}"),
                },
                60_000,
            )
            .await
            .map_err(debug_error)?;
    }
    let keys = Arc::new(PostgresApiKeyStore::new(
        pool.clone(),
        ApiKeyKeyring::new(1, [(1, vec![0x48; 32])]).map_err(debug_error)?,
    ));
    let project = keys
        .create_project("Video lookup fixture")
        .await
        .map_err(debug_error)?;
    let other_project = keys
        .create_project("Other lookup scope")
        .await
        .map_err(debug_error)?;
    let owner = keys
        .create_service_account(
            &project.id,
            "Owner",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_error)?;
    let reader = keys
        .create_service_account(
            &project.id,
            "Reader",
            ApiKeyPermissionMode::ReadOnly,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_error)?;
    let denied = keys
        .create_service_account(
            &project.id,
            "Denied",
            ApiKeyPermissionMode::Restricted,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_error)?;
    let other = keys
        .create_service_account(
            &other_project.id,
            "Other",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_error)?;
    let model = FixtureModel {
        api_profile: PROFILE,
        public_model: "seedance2.5",
        provider_model: "seedance2.5",
        execution_model: "seedance2.5",
        media_kind: "video",
        workflow: None,
        rate_micros: 1000,
    };
    seed_model_route(
        pool,
        &project.id,
        &owner.id,
        &owner.api_key.id,
        profile.provider_account_id,
        profile.execution_profile_id,
        &home,
        model,
    )
    .await?;
    seed_model_economics(pool, &project.id, model).await?;
    let blobs = Arc::new(FilesystemArtifactBlobStore::new(root.path()).map_err(debug_error)?);
    let store = Arc::new(PostgresExecutionSettlementStore::new(
        pool.clone(),
        blobs.clone(),
    ));
    let admission = Arc::new(PostgresAdmissionStore::new(pool.clone()));
    let app = build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing(
        config(), ExternalImageGatewayComponents {
            usage_store: Arc::new(PostgresUsageStore::new(pool.clone())), api_key_store: keys,
            admission_store: admission.clone(), settlement_store: store.clone(), input_blob_store: blobs,
            provider_readiness_store: Arc::new(provider),
        }, None, None, None, None, Some(Arc::new(PostgresModelRoutingStore::new(pool.clone()))),
    ).map_err(debug_error)?;
    let body = json!({"workflow":"text2video","model_version":"seedance2.5","prompt":"private prompt must not leave lookup","duration":5,"video_resolution":"480p","ratio":"16:9"});
    // The durable admission completes, but the caller drops the response body.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/dreamina/videos/generations")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", owner.api_key.value),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", KEY)
                .body(Body::from(body.to_string()))
                .map_err(debug_error)?,
        )
        .await
        .map_err(debug_error)?;
    require(
        response.status() == StatusCode::OK,
        format!("real admission failed: {}", response.status()),
    )?;
    drop(response);
    let original: Uuid = sqlx::query_scalar("SELECT job_id FROM jobs")
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
    let before = counts(pool).await?;
    require(
        before.0 == 1 && before.4 == 0,
        "admission invoked a provider or created multiple jobs",
    )?;
    for _ in 0..3 {
        let (code, found) = json_request(
            app.clone(),
            Method::GET,
            LOOKUP,
            &reader.api_key.value,
            Some(KEY),
            None,
        )
        .await?;
        require(
            code == StatusCode::OK && found["id"] == original.to_string(),
            format!("original ID lookup failed: {code} {found}"),
        )?;
        require(
            found["request"]
                == json!({"workflow":"text2video","model_version":"seedance2.5","video_resolution":"480p","ratio":"16:9","duration_ms":5000,"output_count":1,"image_count":0,"video_count":0,"audio_count":0}),
            format!("wrong frozen request: {found}"),
        )?;
        require(
            found["request_id"].is_string()
                && !found.to_string().contains("private prompt")
                && !found.to_string().contains(KEY),
            "lookup leaked private input or key",
        )?;
    }
    require(
        counts(pool).await? == before,
        "GET created a job, charge, ledger entry, submission or idempotency identity",
    )?;
    for (token, key, uri, expected) in [
        (
            reader.api_key.value.as_str(),
            None,
            LOOKUP,
            StatusCode::BAD_REQUEST,
        ),
        (
            reader.api_key.value.as_str(),
            Some(""),
            LOOKUP,
            StatusCode::BAD_REQUEST,
        ),
        (
            reader.api_key.value.as_str(),
            Some("wrong-key"),
            LOOKUP,
            StatusCode::NOT_FOUND,
        ),
        (
            denied.api_key.value.as_str(),
            Some(KEY),
            LOOKUP,
            StatusCode::FORBIDDEN,
        ),
        (
            other.api_key.value.as_str(),
            Some(KEY),
            LOOKUP,
            StatusCode::NOT_FOUND,
        ),
        ("invalid-token", Some(KEY), LOOKUP, StatusCode::UNAUTHORIZED),
        (
            reader.api_key.value.as_str(),
            Some(KEY),
            "/v1/dreamina/videos/by-idempotency-key?operation=edit",
            StatusCode::BAD_REQUEST,
        ),
        (
            reader.api_key.value.as_str(),
            Some(KEY),
            "/v1/dreamina/images/by-idempotency-key",
            StatusCode::NOT_FOUND,
        ),
    ] {
        let (status, _) = json_request(app.clone(), Method::GET, uri, token, key, None).await?;
        require(
            status == expected,
            format!("scope/key validation {uri}: expected {expected}, got {status}"),
        )?;
    }
    let digest = idempotency_key_digest(&project.id, PROFILE, "video_generation", KEY)
        .map_err(debug_error)?;
    for (tenant, scope, actor) in [
        ("wrong-tenant", project.id.as_str(), None),
        (project.id.as_str(), "wrong-project", None),
        (
            project.id.as_str(),
            project.id.as_str(),
            Some(Uuid::new_v4()),
        ),
    ] {
        require(
            store
                .project_video_job_by_idempotency_key(tenant, scope, actor, &digest)
                .await
                .map_err(debug_error)?
                .is_none(),
            "lookup leaked tenant/project/actor",
        )?;
    }
    // A distinct image identity can receive the exact same raw key without
    // changing the already admitted video or its fixed-key lookup.
    let image_ticket = claim(
        &admission,
        pool,
        &project.id,
        "dreamina-cli-images-v1",
        "generation",
        KEY,
    )
    .await?;
    let (code, snapshot) = json_request(
        app.clone(),
        Method::GET,
        "/v1/dreamina/images/by-idempotency-key",
        &reader.api_key.value,
        Some(KEY),
        None,
    )
    .await?;
    require(
        code == StatusCode::CONFLICT && snapshot["error"]["code"] == "idempotency_in_progress",
        "same key crossed image/video namespace",
    )?;
    admission.abort(&image_ticket).await.map_err(debug_error)?;
    let ticket = claim(
        &admission,
        pool,
        &project.id,
        PROFILE,
        "video_generation",
        "receiving-video-key",
    )
    .await?;
    let before = counts(pool).await?;
    let (code, receiving) = json_request(
        app.clone(),
        Method::GET,
        LOOKUP,
        &reader.api_key.value,
        Some("receiving-video-key"),
        None,
    )
    .await?;
    require(
        code == StatusCode::CONFLICT && receiving["error"]["code"] == "idempotency_in_progress",
        "receiving lookup did not preserve 409",
    )?;
    require(
        counts(pool).await? == before,
        "receiving GET mutated durable state",
    )?;
    admission.abort(&ticket).await.map_err(debug_error)?;
    let (code, conflict) = json_request(
        app.clone(),
        Method::GET,
        LOOKUP,
        &reader.api_key.value,
        Some("receiving-video-key"),
        None,
    )
    .await?;
    require(
        code == StatusCode::CONFLICT && conflict["error"]["code"] == "idempotency_conflict",
        "aborted identity did not fail closed",
    )?;
    let (code, found) = json_request(
        app,
        Method::GET,
        LOOKUP,
        &reader.api_key.value,
        Some(KEY),
        None,
    )
    .await?;
    require(
        code == StatusCode::OK && found["id"] == original.to_string(),
        "other namespace changed original video",
    )
}

async fn claim(
    store: &PostgresAdmissionStore,
    pool: &PgPool,
    project: &str,
    profile: &str,
    operation: &str,
    key: &str,
) -> TestResult<gpt_image_2_gateway::admission::AdmissionTicket> {
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let request = ClaimAdmission {
        owner_token: Uuid::new_v4(),
        tenant_id: project.to_owned(),
        project_id: project.to_owned(),
        api_profile: profile.to_owned(),
        operation: operation.to_owned(),
        request_id: Uuid::new_v4().to_string(),
        idempotency_key_digest: Some(
            idempotency_key_digest(project, profile, operation, key).map_err(debug_error)?,
        ),
        request_hash: "b".repeat(64),
        deadline_at_ms: now + 60_000,
    };
    match store.claim(request).await.map_err(debug_error)? {
        AdmissionClaim::Owner(ticket) => Ok(ticket),
        _ => Err("fixture claim was not owned".into()),
    }
}

async fn counts(pool: &PgPool) -> TestResult<(i64, i64, i64, i64, i64, i64)> {
    sqlx::query_as("SELECT (SELECT count(*) FROM jobs),(SELECT count(*) FROM customer_billing_holds),(SELECT count(*) FROM ledger_transactions),(SELECT count(*) FROM ledger_postings),(SELECT count(*) FROM provider_submissions),(SELECT count(*) FROM idempotency_requests)").fetch_one(pool).await.map_err(debug_error)
}
