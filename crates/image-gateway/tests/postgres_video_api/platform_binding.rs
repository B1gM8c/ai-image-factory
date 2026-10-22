use super::*;
use factory_identity::{
    AccessTokenCodec, AuthPolicy, IdentityService, LoginRequest, RefreshTokenKeyring,
};
use gpt_image_2_gateway::{
    ExternalControlPlaneServices, build_router_with_external_execution_and_services,
    identity::PostgresIdentityRepository, provider_management::PostgresProviderManagementService,
};

// Test-only signing pair shared with the existing identity integration fixtures.
const PRIVATE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgJ6r5c63M0tPZV05C
Y0U72GBHm9iqV7QaUgFxk/9dBn+hRANCAAT5ufmoZxTrAkeOwJFSjVcbQ1Pvl2sw
892/nV1rvRJwDokKy+s00P46StleDgXLe9hOly8yM81frZfcMeI1krz+
-----END PRIVATE KEY-----
"#;
const PUBLIC_KEY: &[u8] = br#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE+bn5qGcU6wJHjsCRUo1XG0NT75dr
MPPdv51da70ScA6JCsvrNND+OkrZXg4Fy3vYTpcvMjPNX62X3DHiNZK8/g==
-----END PUBLIC KEY-----
"#;
const PASSWORD: &str = "platform binding integration fixture";
const OWNER: &str = "binding-owner@example.test";

struct AdminApi {
    app: axum::Router,
    identity: Arc<IdentityService>,
    owner_token: String,
    _artifacts: TempDir,
}

impl AdminApi {
    async fn new(pool: &PgPool, home: &Path) -> TestResult<Self> {
        let policy = AuthPolicy::default();
        let codec = AccessTokenCodec::new(
            "platform-binding-test",
            PRIVATE_KEY,
            [("platform-binding-test".to_owned(), PUBLIC_KEY.to_vec())],
            "https://identity.binding.test",
            "urn:aif:platform-binding-test",
            &policy,
        )
        .map_err(debug_error)?;
        let identity = Arc::new(
            IdentityService::new(
                Arc::new(PostgresIdentityRepository::new(pool.clone())),
                codec,
                RefreshTokenKeyring::new(1, [(1, vec![0x65; 32])]).map_err(debug_error)?,
                policy,
            )
            .map_err(debug_error)?,
        );
        identity
            .bootstrap_admin(
                OWNER.to_owned(),
                "Binding owner".to_owned(),
                PASSWORD.to_owned(),
            )
            .await
            .map_err(debug_error)?;
        let owner_token = login(&identity, OWNER).await?;
        let artifacts = TempDir::new().map_err(debug_error)?;
        let blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifacts.path()).map_err(debug_error)?);
        let app = build_router_with_external_execution_and_services(
            config(),
            ExternalImageGatewayComponents {
                usage_store: Arc::new(PostgresUsageStore::new(pool.clone())),
                api_key_store: Arc::new(PostgresApiKeyStore::new(
                    pool.clone(),
                    ApiKeyKeyring::new(1, [(1, vec![0x44; 32])]).map_err(debug_error)?,
                )),
                admission_store: Arc::new(PostgresAdmissionStore::new(pool.clone())),
                settlement_store: Arc::new(PostgresExecutionSettlementStore::new(
                    pool.clone(),
                    blobs.clone(),
                )),
                input_blob_store: blobs,
                provider_readiness_store: Arc::new(PostgresProviderTaskStore::new(pool.clone())),
            },
            ExternalControlPlaneServices {
                identity_service: Some(identity.clone()),
                provider_management_service: Some(Arc::new(
                    PostgresProviderManagementService::new(
                        pool.clone(),
                        home.to_path_buf(),
                        "/nonexistent/test-cli".into(),
                    ),
                )),
                pricing_admin_service: Some(Arc::new(PostgresPricingAdminService::new(
                    pool.clone(),
                ))),
                ..ExternalControlPlaneServices::default()
            },
        )
        .map_err(debug_error)?;
        Ok(Self {
            app,
            identity,
            owner_token,
            _artifacts: artifacts,
        })
    }

    async fn request(
        &self,
        method: Method,
        route: Uuid,
        body: Option<&Value>,
    ) -> TestResult<(StatusCode, Value)> {
        json_request(
            self.app.clone(),
            method,
            &binding_path(route),
            &self.owner_token,
            None,
            body,
        )
        .await
    }
}

async fn login(identity: &IdentityService, email: &str) -> TestResult<String> {
    Ok(identity
        .login(LoginRequest {
            email: email.to_owned(),
            password: PASSWORD.to_owned(),
            client_id: AuthPolicy::default().client_id,
        })
        .await
        .map_err(debug_error)?
        .access_token)
}

fn binding_path(route: Uuid) -> String {
    format!("/admin/v1/provider-routes/{route}/platform-binding")
}

pub(super) async fn publish_fixture_route(pool: &PgPool, home: &Path, route: Uuid) -> TestResult {
    let admin = AdminApi::new(pool, home).await?;
    let (status, body) = admin
        .request(
            Method::PUT,
            route,
            Some(&json!({
                "expected_route_revision": 1, "expected_binding": null,
            })),
        )
        .await?;
    require(
        status == StatusCode::OK && body["binding"] == binding(route, 1),
        format!("owner HTTP platform route publication failed: {status} {body}"),
    )
}

fn binding(route: Uuid, revision: i64) -> Value {
    json!({"route_id": route, "route_revision": revision, "state": "enabled"})
}

#[tokio::test]
async fn platform_binding_owner_api_enforces_cas_and_unblocks_price_coverage() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = run_binding_case(&database.pool).await;
    combine(result, database.cleanup().await)
}

async fn run_binding_case(pool: &PgPool) -> TestResult {
    let home = TempDir::new().map_err(debug_error)?;
    let provisioned = provision_dreamina_video_execution_profile(
        pool,
        &DreaminaExecutionProfileProvisioning {
            profile_key: "binding-video-fixture".to_owned(),
            credential_pool_key: "binding-video-fixture".to_owned(),
            provider_account_key: "binding-video-fixture".to_owned(),
            credential_ref: "private:binding-video-fixture".to_owned(),
            credential_revision: 1,
            credential_auth_sha256: "a".repeat(64),
            max_concurrency: 1,
        },
    )
    .await
    .map_err(debug_error)?;
    let keys = PostgresApiKeyStore::new(
        pool.clone(),
        ApiKeyKeyring::new(1, [(1, vec![0x44; 32])]).map_err(debug_error)?,
    );
    let project = keys
        .create_project("Platform binding test")
        .await
        .map_err(debug_error)?;
    let key = keys
        .create_service_account(
            &project.id,
            "Pinned key",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_error)?;
    let route = seed_model_route_unbound(
        pool,
        &project.id,
        &key.id,
        &key.api_key.id,
        provisioned.provider_account_id,
        provisioned.execution_profile_id,
        home.path(),
        FixtureModel::legacy(),
    )
    .await?;
    let pinned_route = route;
    sqlx::query("INSERT INTO gateway_project_provider_routes (project_id,provider_id,operation_id,command_schema,route_id,route_revision,state,created_at_ms,updated_at_ms) VALUES ($1,'dreamina-cli','videos.generations',$2,$3,1,'enabled',1,1)")
        .bind(&project.id).bind(DREAMINA_SUBMIT_COMMAND_SCHEMA).bind(route)
        .execute(pool).await.map_err(debug_error)?;
    let admin = AdminApi::new(pool, home.path()).await?;
    let first_request = json!({"expected_route_revision": 1, "expected_binding": null});
    let (status, before) = admin.request(Method::GET, route, None).await?;
    require(
        status == StatusCode::OK && before == json!({"binding":null}),
        "unbound route must report null",
    )?;
    let pricing = PostgresPricingAdminService::new(pool.clone());
    let coverage = pricing.coverage().await.map_err(debug_error)?;
    require(
        coverage.rows.iter().any(|row| {
            row.provider_id == "dreamina-cli"
                && row.provider_model_id == "seedance2.5"
                && row.route_status == "missing"
                && row
                    .blocking_reasons
                    .iter()
                    .any(|reason| reason == "platform_route_missing")
        }),
        "unbound Dreamina model must fail price coverage",
    )?;

    admin
        .identity
        .create_member_user(
            "binding-member@example.test".to_owned(),
            "Member".to_owned(),
            PASSWORD.to_owned(),
        )
        .await
        .map_err(debug_error)?;
    let member = login(&admin.identity, "binding-member@example.test").await?;
    for token in [&member, "video-e2e-admin"] {
        for method in [Method::GET, Method::PUT] {
            let (status, _) = json_request(
                admin.app.clone(),
                method,
                &binding_path(route),
                token,
                None,
                Some(&first_request),
            )
            .await?;
            require(
                status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED,
                format!("non-owner platform binding request unexpectedly allowed: {status}"),
            )?;
        }
    }
    // Having admin:* does not turn a workspace member into a platform owner.
    sqlx::query("UPDATE identity_users SET scopes=ARRAY['admin:*'], authz_version=authz_version+1 WHERE normalized_email='binding-member@example.test'")
        .execute(pool).await.map_err(debug_error)?;
    let scoped_member = login(&admin.identity, "binding-member@example.test").await?;
    for method in [Method::GET, Method::PUT] {
        let (status, _) = json_request(
            admin.app.clone(),
            method,
            &binding_path(route),
            &scoped_member,
            None,
            Some(&first_request),
        )
        .await?;
        require(
            status == StatusCode::FORBIDDEN,
            "admin:* without platform_owner must remain forbidden",
        )?;
    }
    let competing =
        create_route(&admin, provisioned.provider_account_id, "competing-video").await?;
    let (first, second) = tokio::join!(
        admin.request(Method::PUT, route, Some(&first_request)),
        admin.request(Method::PUT, competing, Some(&first_request)),
    );
    let first = first?;
    let second = second?;
    require(
        (first.0 == StatusCode::OK && second.0 == StatusCode::CONFLICT)
            || (first.0 == StatusCode::CONFLICT && second.0 == StatusCode::OK),
        format!("concurrent first publication must have exactly one winner: {first:?} {second:?}"),
    )?;
    let (route, created) = if first.0 == StatusCode::OK {
        (route, first.1)
    } else {
        (competing, second.1)
    };
    require(
        created["binding"] == binding(route, 1),
        "first binding returned wrong winning route",
    )?;
    let (status, replay) = admin
        .request(Method::PUT, route, Some(&first_request))
        .await?;
    require(
        status == StatusCode::OK && replay == created,
        "identical initial request must replay the binding",
    )?;
    assert_audit(pool, &admin, 1, Value::Null, binding(route, 1)).await?;
    seed_economics(pool, &project.id).await?;
    let coverage = pricing.coverage().await.map_err(debug_error)?;
    require(
        coverage.rows.iter().any(|row| {
            row.provider_id == "dreamina-cli"
                && row.public_model_id.as_deref() == Some(MODEL)
                && row.route_status == "routable"
                && row.customer_price_status == "ready"
                && row.blocking_reasons.is_empty()
        }),
        format!("published route and price must complete coverage: {coverage:?}"),
    )?;

    let (status, updated) = json_request(
        admin.app.clone(),
        Method::PUT,
        &format!("/admin/v1/provider-routes/{route}"),
        &admin.owner_token,
        None,
        Some(
            &json!({"expected_revision":1, "display_name":"Saved revision two",
            "members":[{"provider_account_id":provisioned.provider_account_id}]}),
        ),
    )
    .await?;
    require(
        status == StatusCode::OK && updated["revision"] == 2,
        format!("route update failed: {status} {updated}"),
    )?;
    let (status, _) = admin
        .request(Method::PUT, route, Some(&first_request))
        .await?;
    require(
        status == StatusCode::CONFLICT,
        "old request must fail after route head advances, including otherwise-idempotent replay",
    )?;
    let (_, current) = admin.request(Method::GET, route, None).await?;
    require(
        current == created,
        "saving route must not automatically repoint platform binding",
    )?;
    let (status, stale) = admin
        .request(
            Method::PUT,
            route,
            Some(&json!({"expected_route_revision":2, "expected_binding":null})),
        )
        .await?;
    require(
        status == StatusCode::CONFLICT,
        format!("null expected binding must not overwrite existing row: {status} {stale}"),
    )?;
    let next_request = json!({"expected_route_revision":2, "expected_binding":created["binding"]});
    let (status, next) = admin
        .request(Method::PUT, route, Some(&next_request))
        .await?;
    require(
        status == StatusCode::OK && next["binding"] == binding(route, 2),
        format!("binding revision update failed: {status} {next}"),
    )?;
    let (status, next_replay) = admin
        .request(Method::PUT, route, Some(&next_request))
        .await?;
    require(
        status == StatusCode::OK && next_replay == next,
        "update replay must not conflict",
    )?;
    assert_audit(pool, &admin, 2, binding(route, 1), binding(route, 2)).await?;

    let replacement =
        create_route(&admin, provisioned.provider_account_id, "replacement-video").await?;
    let (status, _) = admin
        .request(
            Method::PUT,
            replacement,
            Some(&json!({"expected_route_revision":1,"expected_binding":created["binding"]})),
        )
        .await?;
    require(
        status == StatusCode::CONFLICT,
        "stale existing binding must not replace a newer platform choice",
    )?;
    let replacement_request =
        json!({"expected_route_revision":1,"expected_binding":next["binding"]});
    let (status, replaced) = admin
        .request(Method::PUT, replacement, Some(&replacement_request))
        .await?;
    require(
        status == StatusCode::OK && replaced["binding"] == binding(replacement, 1),
        format!("replacement failed: {status} {replaced}"),
    )?;
    assert_audit(pool, &admin, 3, binding(route, 2), binding(replacement, 1)).await?;
    let pinned: (Uuid,i64) = sqlx::query_as("SELECT route_id, route_revision FROM gateway_api_key_provider_routes WHERE api_key_id = $1")
        .bind(&key.api_key.id).fetch_one(pool).await.map_err(debug_error)?;
    require(
        pinned == (pinned_route, 1),
        "platform change must preserve API key's pinned route revision",
    )?;

    let pinned_project: (Uuid, i64) = sqlx::query_as(
        "SELECT route_id,route_revision FROM gateway_project_provider_routes WHERE project_id=$1",
    )
    .bind(&project.id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        pinned_project == (pinned_route, 1),
        "platform change must preserve project pinned route",
    )?;

    sqlx::query("UPDATE provider_route_heads SET state='disabled' WHERE route_id=$1")
        .bind(replacement)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    let (status, _) = admin
        .request(Method::PUT, replacement, Some(&replacement_request))
        .await?;
    require(
        status.is_client_error(),
        "disabled target must reject a replay",
    )?;
    sqlx::query("UPDATE provider_route_heads SET state='enabled' WHERE route_id=$1")
        .bind(replacement)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    sqlx::query("UPDATE provider_models SET lifecycle_state='disabled' WHERE provider_id='dreamina-cli' AND model_id='seedance2.5'").execute(pool).await.map_err(debug_error)?;
    let (status, _) = admin
        .request(Method::PUT, replacement, Some(&replacement_request))
        .await?;
    require(
        status.is_client_error(),
        "unroutable mapping must reject a replay",
    )?;
    sqlx::query("UPDATE provider_models SET lifecycle_state='enabled' WHERE provider_id='dreamina-cli' AND model_id='seedance2.5'").execute(pool).await.map_err(debug_error)?;
    sqlx::query(
        "UPDATE provider_execution_profiles SET state='disabled' WHERE execution_profile_id=$1",
    )
    .bind(provisioned.execution_profile_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let (status, _) = admin
        .request(Method::PUT, replacement, Some(&replacement_request))
        .await?;
    require(
        status.is_client_error(),
        "disabled execution profile must reject publication replay",
    )?;
    assert_audit(pool, &admin, 3, binding(route, 2), binding(replacement, 1)).await
}

async fn create_route(admin: &AdminApi, account: Uuid, key: &str) -> TestResult<Uuid> {
    let (status, response) = json_request(admin.app.clone(), Method::POST, "/admin/v1/provider-routes",
        &admin.owner_token, None, Some(&json!({"route_key":key, "display_name":"Platform binding candidate",
        "provider_id":"dreamina-cli", "operation_id":"videos.generations",
        "members":[{"provider_account_id":account}],
        "model_mappings":[{"api_profile":"volcengine-ark-content-generation-v3", "public_model_id":MODEL,
            "provider_model_id":"seedance2.5", "media_kind":"video"}]}))).await?;
    require(
        status == StatusCode::OK,
        format!("candidate route creation failed: {status} {response}"),
    )?;
    Uuid::parse_str(
        response["route_id"]
            .as_str()
            .ok_or("candidate route missing id")?,
    )
    .map_err(debug_error)
}

async fn assert_audit(
    pool: &PgPool,
    admin: &AdminApi,
    expected_count: usize,
    before: Value,
    after: Value,
) -> TestResult {
    let principal = admin
        .identity
        .authenticate_access(&admin.owner_token)
        .await
        .map_err(debug_error)?;
    let rows: Vec<(Uuid, Uuid, Value)> = sqlx::query_as("SELECT actor_user_id, session_id, metadata FROM identity_audit_events WHERE action='provider_route.platform_binding.update' ORDER BY created_at_ms, event_id")
        .fetch_all(pool).await.map_err(debug_error)?;
    require(
        rows.len() == expected_count
            && rows.iter().all(|(actor, session, _)| {
                *actor == principal.user_id && *session == principal.session_id
            })
            && rows
                .iter()
                .any(|(_, _, metadata)| metadata["before"] == before && metadata["after"] == after),
        format!(
            "platform binding must audit each mutation exactly once with authenticated actor and before/after: {rows:?}"
        ),
    )
}
