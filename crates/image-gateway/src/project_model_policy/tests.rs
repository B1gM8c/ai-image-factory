use std::{env, sync::Arc};

use factory_identity::{AccessTokenCodec, AuthPolicy, IdentityService, RefreshTokenKeyring};
use image_provider_contracts::BillingMetric;
use sqlx::{AssertSqlSafe, PgPool};
use tokio::task::JoinSet;
use uuid::Uuid;

use super::*;
use crate::{
    auth::{RequestAttribution, RequestRouteAttribution},
    database::{connect_test_pool_with_search_path, run_migrations},
    identity::PostgresIdentityRepository,
    usage::UsageLimits,
};

type TestResult<T = ()> = Result<T, String>;

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
const PASSWORD: &str = "correct horse battery staple";

#[tokio::test]
async fn policy_versions_shared_buckets_and_native_units_are_fail_closed() -> TestResult {
    let Some(schema) = TestSchema::new(12).await? else {
        return Ok(());
    };
    let result = policy_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

async fn policy_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("project model policy migrations failed: {error:?}"))?;
    let identity = identity_service(pool)?;
    identity
        .bootstrap_admin(
            "project-model-owner@limits.test".to_string(),
            "Project Model Owner".to_string(),
            PASSWORD.to_string(),
        )
        .await
        .map_err(debug_error)?;
    let owner = identity
        .list_users(None, 10)
        .await
        .map_err(debug_error)?
        .into_iter()
        .find(|user| user.email == "project-model-owner@limits.test")
        .ok_or_else(|| "bootstrapped project model owner is missing".to_string())?;
    let default_project = owner
        .projects
        .first()
        .ok_or_else(|| "bootstrapped project model owner has no project".to_string())?;
    let organization_id: String =
        sqlx::query_scalar("SELECT tenant_id FROM gateway_projects WHERE id = $1")
            .bind(&default_project.project_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let routes = available_routes();
    let service = PostgresProjectModelPolicyService::new(pool.clone());

    let fresh = service
        .get_policy(&default_project.project_id, routes.clone())
        .await
        .map_err(debug_error)?;
    require(
        !fresh.configured
            && fresh.control_version == "0"
            && fresh.models.len() == routes.len()
            && fresh.models.iter().all(|model| model.allowed),
        "fresh policy did not allow all currently routable models",
    )?;

    sqlx::query(
        r#"
        UPDATE platform_model_limit_members
        SET request_ceiling_per_minute = 2,
            unit_ceiling_per_minute = 100
        WHERE bucket_key = 'openai:gpt-image-2'
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let inherited_project = insert_project(pool, &organization_id, "inherited-limit").await?;
    let now_ms = 1_000_000;
    for route in [&routes[0], &routes[1]] {
        admit(
            pool,
            usage_charge(
                &organization_id,
                &inherited_project,
                route,
                Uuid::new_v4(),
                1,
                1,
            ),
            now_ms,
        )
        .await
        .map_err(debug_error)?;
    }
    let inherited_exceeded = admit(
        pool,
        usage_charge(
            &organization_id,
            &inherited_project,
            &routes[0],
            Uuid::new_v4(),
            1,
            1,
        ),
        now_ms,
    )
    .await
    .expect_err("inherited platform request limit must reject excess requests");
    require(
        inherited_exceeded.status_code().as_u16() == 429,
        "inherited platform request limit did not return 429",
    )?;
    let inherited_override_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_model_rate_limits WHERE project_id = $1")
            .bind(&inherited_project)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        inherited_override_count == 0,
        "inherited platform limit was incorrectly materialized as a project override",
    )?;
    sqlx::query(
        r#"
        UPDATE platform_model_limit_members
        SET request_ceiling_per_minute = 100,
            unit_ceiling_per_minute = 100
        WHERE bucket_key = 'openai:gpt-image-2'
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let configured = service
        .update_policy(
            &default_project.project_id,
            owner.user_id,
            routes.clone(),
            policy_request(&routes[..2], &routes[..2], Some((2, 100)), "0"),
        )
        .await
        .map_err(debug_error)?;
    let image_models = configured
        .models
        .iter()
        .filter(|model| model.model.media_kind == "image")
        .collect::<Vec<_>>();
    require(
        configured.configured
            && configured.control_version == "1"
            && image_models.len() == 2
            && image_models.iter().all(|model| {
                model.allowed
                    && model.rate_limit.shared
                    && model.rate_limit.bucket_key == "openai:gpt-image-2"
                    && model.rate_limit.request_limit_per_minute == Some(2)
            }),
        "shared image aliases did not expose one effective rate-limit bucket",
    )?;

    let stale = service
        .update_policy(
            &default_project.project_id,
            owner.user_id,
            routes.clone(),
            policy_request(&routes[..2], &routes[..2], Some((2, 100)), "0"),
        )
        .await
        .expect_err("stale model policy version must conflict");
    require(
        stale.status_code().as_u16() == 409,
        "stale model policy update did not return conflict",
    )?;

    let conflict = service
        .update_policy(
            &default_project.project_id,
            owner.user_id,
            routes.clone(),
            UpdateProjectModelPolicyRequest {
                allowed_models: routes[..2].iter().map(model_identity).collect(),
                rate_limits: vec![
                    rate_limit(&routes[0], 2, 100),
                    rate_limit(&routes[1], 3, 100),
                ],
                expected_control_version: "1".to_string(),
            },
        )
        .await
        .expect_err("shared aliases must reject conflicting limits");
    require(
        conflict.status_code().as_u16() == 400,
        "conflicting shared alias limits were not rejected",
    )?;

    let mut attempts = JoinSet::new();
    for index in 0..8 {
        let pool = pool.clone();
        let project_id = default_project.project_id.clone();
        let organization_id = organization_id.clone();
        let route = routes[index % 2].clone();
        attempts.spawn(async move {
            let session_id = Uuid::new_v4();
            let result = admit(
                &pool,
                usage_charge(&organization_id, &project_id, &route, session_id, 1, 1),
                now_ms,
            )
            .await;
            (session_id, result)
        });
    }
    let mut accepted = Vec::new();
    let mut rejected = 0;
    while let Some(attempt) = attempts.join_next().await {
        let (session_id, result) = attempt.map_err(debug_error)?;
        match result {
            Ok(()) => accepted.push(session_id),
            Err(error) if error.status_code().as_u16() == 429 => rejected += 1,
            Err(error) => return Err(format!("unexpected rate-limit result: {error:?}")),
        }
    }
    require(
        accepted.len() == 2 && rejected == 6,
        "concurrent aliases exceeded their shared two-request capacity",
    )?;

    let admission_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_model_rate_admissions WHERE project_id = $1",
    )
    .bind(&default_project.project_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        admission_count == 2,
        "accepted requests were not recorded exactly once",
    )?;
    admit(
        pool,
        usage_charge(
            &organization_id,
            &default_project.project_id,
            &routes[0],
            accepted[0],
            1,
            1,
        ),
        now_ms,
    )
    .await
    .map_err(debug_error)?;
    let replay_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_model_rate_admissions WHERE project_id = $1",
    )
    .bind(&default_project.project_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        replay_count == admission_count,
        "idempotent admission consumed rate capacity twice",
    )?;

    let unchanged = service
        .update_policy(
            &default_project.project_id,
            owner.user_id,
            routes.clone(),
            policy_request(&routes[..2], &routes[..2], Some((2, 100)), "1"),
        )
        .await
        .map_err(debug_error)?;
    require(
        unchanged.control_version == "2",
        "valid policy update did not advance the control version",
    )?;
    let preserved_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_model_rate_admissions WHERE project_id = $1",
    )
    .bind(&default_project.project_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        preserved_count == admission_count,
        "unchanged rate-limit update reset live bucket state",
    )?;

    service
        .update_policy(
            &default_project.project_id,
            owner.user_id,
            routes.clone(),
            policy_request(&routes[..1], &[], None, "2"),
        )
        .await
        .map_err(debug_error)?;
    let denied = admit(
        pool,
        usage_charge(
            &organization_id,
            &default_project.project_id,
            &routes[1],
            Uuid::new_v4(),
            1,
            1,
        ),
        now_ms,
    )
    .await
    .expect_err("unlisted model must be denied");
    require(
        denied.status_code().as_u16() == 404,
        "unlisted model did not use OpenAI-compatible not-found isolation",
    )?;

    let image_unit_project = insert_project(pool, &organization_id, "image-unit").await?;
    service
        .update_policy(
            &image_unit_project,
            owner.user_id,
            routes.clone(),
            policy_request(&routes[..2], &routes[..2], Some((100, 2)), "0"),
        )
        .await
        .map_err(debug_error)?;
    admit(
        pool,
        usage_charge(
            &organization_id,
            &image_unit_project,
            &routes[0],
            Uuid::new_v4(),
            2,
            2,
        ),
        now_ms,
    )
    .await
    .map_err(debug_error)?;
    let image_units_exceeded = admit(
        pool,
        usage_charge(
            &organization_id,
            &image_unit_project,
            &routes[1],
            Uuid::new_v4(),
            1,
            1,
        ),
        now_ms,
    )
    .await
    .expect_err("image output count must consume native image units");
    require(
        image_units_exceeded.status_code().as_u16() == 429,
        "image-unit limit did not reject the next output",
    )?;

    let video_unit_project = insert_project(pool, &organization_id, "video-unit").await?;
    service
        .update_policy(
            &video_unit_project,
            owner.user_id,
            routes.clone(),
            policy_request(&routes[2..], &routes[2..], Some((100, 5)), "0"),
        )
        .await
        .map_err(debug_error)?;
    admit(
        pool,
        usage_charge(
            &organization_id,
            &video_unit_project,
            &routes[2],
            Uuid::new_v4(),
            1,
            4,
        ),
        now_ms,
    )
    .await
    .map_err(debug_error)?;
    let video_units_exceeded = admit(
        pool,
        usage_charge(
            &organization_id,
            &video_unit_project,
            &routes[3],
            Uuid::new_v4(),
            1,
            2,
        ),
        now_ms,
    )
    .await
    .expect_err("video duration must consume native video-second units");
    require(
        video_units_exceeded.status_code().as_u16() == 429,
        "video-second limit did not reject excess duration",
    )?;

    let corrupt_project = insert_project(pool, &organization_id, "media-mismatch").await?;
    let mut corrupt_route = routes[0].clone();
    corrupt_route.media_kind = "video".to_string();
    let corrupt = admit(
        pool,
        usage_charge(
            &organization_id,
            &corrupt_project,
            &corrupt_route,
            Uuid::new_v4(),
            1,
            1,
        ),
        now_ms,
    )
    .await
    .expect_err("platform model media mismatch must fail closed");
    require(
        corrupt.status_code().is_server_error(),
        "platform model media mismatch did not fail closed",
    )
}

async fn admit(pool: &PgPool, charge: UsageCharge, now_ms: i64) -> Result<(), ImageGatewayError> {
    let mut tx = pool.begin().await.map_err(unavailable)?;
    match enforce_project_model_controls(&mut tx, &charge, now_ms).await {
        Ok(()) => tx.commit().await.map_err(unavailable),
        Err(error) => {
            tx.rollback().await.map_err(unavailable)?;
            Err(error)
        }
    }
}

fn usage_charge(
    organization_id: &str,
    project_id: &str,
    route: &PublicModelRoute,
    admission_session_id: Uuid,
    output_count: u32,
    billable_units: u32,
) -> UsageCharge {
    let billing_metric = if route.media_kind == "video" {
        BillingMetric::VideoSecond
    } else {
        BillingMetric::Output
    };
    UsageCharge {
        tenant_id: organization_id.to_string(),
        attribution: Some(RequestAttribution {
            project_id: project_id.to_string(),
            service_account_id: None,
            api_key_id: None,
            credential_authz_version: None,
            credential_owner_user_id: None,
            actor_user_id: None,
            actor_session_id: None,
            actor_authz_version: None,
            route: Some(RequestRouteAttribution {
                public_model_id: route.id.clone(),
                api_profile: route.api_profile.clone(),
                provider_id: route.provider_id.clone(),
                operation_id: route.operation_id.clone(),
                command_schema: format!("{}.v1", route.operation_id),
                media_kind: route.media_kind.clone(),
                route_id: Uuid::new_v4(),
                route_revision: 1,
            }),
        }),
        request_id: format!("req_{}", Uuid::new_v4().simple()),
        admission_session_id: Some(admission_session_id),
        operation: if route.media_kind == "video" {
            "video.generate"
        } else {
            "image.generate"
        },
        provider_id: route.provider_id.clone(),
        model: route.id.clone(),
        output_count,
        billable_units,
        billing_metric,
        limits: UsageLimits {
            five_hour_image_limit: 1_000_000,
            seven_day_image_limit: 1_000_000,
        },
    }
}

fn policy_request(
    allowed: &[PublicModelRoute],
    limited: &[PublicModelRoute],
    limits: Option<(u32, u32)>,
    expected_control_version: &str,
) -> UpdateProjectModelPolicyRequest {
    UpdateProjectModelPolicyRequest {
        allowed_models: allowed.iter().map(model_identity).collect(),
        rate_limits: limits
            .map(|(requests, units)| {
                limited
                    .iter()
                    .map(|model| rate_limit(model, requests, units))
                    .collect()
            })
            .unwrap_or_default(),
        expected_control_version: expected_control_version.to_string(),
    }
}

fn rate_limit(model: &PublicModelRoute, requests: u32, units: u32) -> UpdateProjectModelRateLimit {
    UpdateProjectModelRateLimit {
        model: model_identity(model),
        request_limit_per_minute: Some(requests),
        unit_limit_per_minute: Some(units),
    }
}

fn model_identity(model: &PublicModelRoute) -> ProjectModelIdentity {
    ProjectModelIdentity {
        operation_id: model.operation_id.clone(),
        api_profile: model.api_profile.clone(),
        public_model_id: model.id.clone(),
        media_kind: model.media_kind.clone(),
    }
}

fn available_routes() -> Vec<PublicModelRoute> {
    vec![
        public_route(
            "gpt-image-2",
            "openai-images-v1",
            "codex",
            "images.generations",
            "image",
        ),
        public_route(
            "gpt-image-2-2026-04-21",
            "openai-images-v1",
            "codex",
            "images.generations",
            "image",
        ),
        public_route(
            "seedance2.0",
            "dreamina-cli-videos-v1",
            "dreamina",
            "videos.generations",
            "video",
        ),
        public_route(
            "doubao-seedance-2-0-260128",
            "volcengine-ark-content-generation-v3",
            "dreamina",
            "videos.generations",
            "video",
        ),
    ]
}

fn public_route(
    id: &str,
    api_profile: &str,
    provider_id: &str,
    operation_id: &str,
    media_kind: &str,
) -> PublicModelRoute {
    PublicModelRoute {
        id: id.to_string(),
        provider_model_id: None,
        api_profile: api_profile.to_string(),
        provider_id: provider_id.to_string(),
        operation_id: operation_id.to_string(),
        media_kind: media_kind.to_string(),
        created_at_ms: 1,
    }
}

async fn insert_project(pool: &PgPool, organization_id: &str, suffix: &str) -> TestResult<String> {
    let project_id = format!(
        "proj_{}_{}",
        suffix.replace('-', "_"),
        Uuid::new_v4().simple()
    );
    sqlx::query(
        "INSERT INTO gateway_projects(id, tenant_id, name, created_at) VALUES ($1, $2, $3, 1)",
    )
    .bind(&project_id)
    .bind(organization_id)
    .bind(format!("Project {suffix}"))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(project_id)
}

fn identity_service(pool: &PgPool) -> TestResult<Arc<IdentityService>> {
    let policy = AuthPolicy::default();
    let access_tokens = AccessTokenCodec::new(
        "project-model-policy-test",
        PRIVATE_KEY,
        [("project-model-policy-test".to_string(), PUBLIC_KEY.to_vec())],
        "https://identity.project-model-policy.test",
        "urn:aif:admin",
        &policy,
    )
    .map_err(debug_error)?;
    let refresh_tokens = RefreshTokenKeyring::new(1, [(1, vec![0x6b; 32])]).map_err(debug_error)?;
    IdentityService::new(
        Arc::new(PostgresIdentityRepository::new(pool.clone())),
        access_tokens,
        refresh_tokens,
        policy,
    )
    .map(Arc::new)
    .map_err(debug_error)
}

fn require(condition: bool, message: &str) -> TestResult {
    condition.then_some(()).ok_or_else(|| message.to_string())
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

struct TestSchema {
    name: String,
    pool: PgPool,
}

impl TestSchema {
    async fn new(max_connections: u32) -> TestResult<Option<Self>> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!(
                "skipping PostgreSQL project model policy test: TEST_DATABASE_URL is not set"
            );
            return Ok(None);
        };
        let name = format!("project_model_policy_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, max_connections, &name)
            .await
            .map_err(debug_error)?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because database {database_name:?} is not a test database"
            ));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .map_err(debug_error);
        self.pool.close().await;
        result.map(|_| ())
    }
}
