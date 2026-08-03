use std::{collections::BTreeMap, env, path::PathBuf, str::FromStr, time::Duration};

use gpt_image_2_gateway::{
    ApiKeyCapability, ApiKeyKeyring, ApiKeyPermissionLevel, ApiKeyPermissionMode,
    ApiKeyPermissions, ApiKeyStore, ImageGatewayError, PostgresApiKeyStore, PostgresUsageStore,
    UsageCharge, UsageLimits, UsageStore,
    admission::{
        AdmissionClaim, AdmissionContract, AdmissionError, AdmissionStore, AttachJob,
        ClaimAdmission, PostgresAdmissionStore,
    },
    database::run_migrations,
    model_routing::{ModelRoutingStore, PostgresModelRoutingStore},
    provider_management::{
        CreateProviderRouteMemberRequest, PostgresProviderManagementService,
        ProviderManagementService, UpdateProviderAccountSchedulingRequest,
        UpdateProviderRouteRequest, reconcile_execution_profile_routes,
    },
    service_tiers::ProjectServiceTier,
};
use image_provider_contracts::BillingMetric;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{sync::Barrier, task::JoinHandle, time::timeout};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const DATABASE_URL: &str = "TEST_DATABASE_URL";
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn queued_revoke_wins_over_queued_authentication() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = revoke_authentication_race(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn authentication_stays_rejected_after_delete() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = sequential_authentication_and_delete(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn service_account_key_lifecycle_preserves_account_and_audits_actor() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let project_id = format!("proj_{}", Uuid::new_v4().simple());
        let actor_user_id = Uuid::new_v4();
        insert_project(&database.setup_pool, &project_id).await?;
        insert_project_member(&database.setup_pool, &project_id, actor_user_id).await?;
        let store = PostgresApiKeyStore::new(database.pool("single_revoke").await?, test_keyring());
        let permissions = ApiKeyPermissions(BTreeMap::from([
            ("models".to_string(), ApiKeyPermissionLevel::Read),
            ("images".to_string(), ApiKeyPermissionLevel::Read),
            ("videos".to_string(), ApiKeyPermissionLevel::None),
        ]));
        let created = store
            .create_service_account_for_actor(
                &project_id,
                "Service account lifecycle",
                actor_user_id,
                ApiKeyPermissionMode::Restricted,
                permissions.clone(),
            )
            .await
            .map_err(|error| format!("failed to create service account: {error:?}"))?;
        let initial_auth = store
            .authenticate(&created.api_key.value)
            .await
            .map_err(|error| format!("failed to authenticate new key: {error:?}"))?
            .ok_or_else(|| "new API key did not authenticate".to_string())?;
        initial_auth
            .require_api_key_capability(ApiKeyCapability::ModelsRead)
            .map_err(|error| format!("new restricted key models read was denied: {error:?}"))?;
        require(
            initial_auth
                .require_api_key_capability(ApiKeyCapability::ImagesWrite)
                .is_err(),
            "new restricted key had an all-permissions window".to_string(),
        )?;
        let updated = store
            .update_project_api_key(
                &project_id,
                &created.api_key.id,
                actor_user_id,
                true,
                "Read-only service key",
                ApiKeyPermissionMode::Restricted,
                permissions,
            )
            .await
            .map_err(|error| format!("failed to update service account key: {error:?}"))?;
        require(
            updated.name == "Read-only service key",
            "service account key update did not persist its name".to_string(),
        )?;
        let updated_auth = store
            .authenticate(&created.api_key.value)
            .await
            .map_err(|error| format!("failed to authenticate updated service key: {error:?}"))?
            .ok_or_else(|| "updated service account key did not authenticate".to_string())?;
        updated_auth
            .require_api_key_capability(ApiKeyCapability::ModelsRead)
            .map_err(|error| format!("updated service key models read was denied: {error:?}"))?;
        require(
            updated_auth
                .require_api_key_capability(ApiKeyCapability::ImagesWrite)
                .is_err(),
            "updated service key unexpectedly retained image write access".to_string(),
        )?;
        let rotated = store
            .rotate_project_api_key(&project_id, &created.api_key.id, actor_user_id, true)
            .await
            .map_err(|error| format!("failed to rotate service account key: {error:?}"))?;
        require(
            store
                .authenticate(&created.api_key.value)
                .await
                .map_err(|error| format!("failed to reject replaced service key: {error:?}"))?
                .is_none(),
            "replaced service account key still authenticated after rotation".to_string(),
        )?;
        require(
            store
                .authenticate(&rotated.api_key.value)
                .await
                .map_err(|error| format!("failed to authenticate rotated service key: {error:?}"))?
                .is_some(),
            "rotated service account key did not authenticate".to_string(),
        )?;
        store
            .delete_project_api_key_for_actor(&project_id, &rotated.api_key.id, actor_user_id)
            .await
            .map_err(|error| format!("failed to revoke API key: {error:?}"))?;
        require(
            store
                .authenticate(&rotated.api_key.value)
                .await
                .map_err(|error| format!("failed to authenticate revoked key: {error:?}"))?
                .is_none(),
            "revoked API key still authenticated".to_string(),
        )?;
        let service_account_active: bool = sqlx::query_scalar(
            "SELECT deleted_at IS NULL FROM gateway_service_accounts WHERE id = $1",
        )
        .bind(&created.id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect service account: {error}"))?;
        require(
            service_account_active,
            "individual key revoke deleted its service account".to_string(),
        )?;
        let audit_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM identity_audit_events
            WHERE actor_user_id = $1
              AND (
                (
                  action = 'project.service_account.create'
                  AND resource_id = $2
                  AND metadata->>'api_key_id' = $3
                )
                OR (
                  action IN (
                    'project.api_key.update',
                    'project.api_key.rotate',
                    'project.api_key.revoke'
                  )
                  AND resource_id IN ($3, $4)
                )
              )
            "#,
        )
        .bind(actor_user_id)
        .bind(&created.id)
        .bind(&created.api_key.id)
        .bind(&rotated.api_key.id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect service key audit events: {error}"))?;
        require(
            audit_count == 4,
            "service account key lifecycle audit events were incomplete".to_string(),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn personal_api_key_is_user_scoped_and_membership_bound() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let project_id = format!("proj_{}", Uuid::new_v4().simple());
        let user_id = Uuid::new_v4();
        insert_project(&database.setup_pool, &project_id).await?;
        insert_project_member(&database.setup_pool, &project_id, user_id).await?;
        let store = PostgresApiKeyStore::new(
            database.pool("personal_key").await?,
            test_keyring(),
        );
        let permissions = ApiKeyPermissions(BTreeMap::from([
            ("models".to_string(), ApiKeyPermissionLevel::Read),
            ("images".to_string(), ApiKeyPermissionLevel::Write),
            ("videos".to_string(), ApiKeyPermissionLevel::None),
        ]));
        let created = store
            .create_user_api_key(
                &project_id,
                user_id,
                "Personal key owner",
                "owner@personal-key.test",
                "Local development",
                ApiKeyPermissionMode::Restricted,
                permissions,
            )
            .await
            .map_err(|error| format!("failed to create personal API key: {error:?}"))?;
        let auth = store
            .authenticate(&created.value)
            .await
            .map_err(|error| format!("failed to authenticate personal API key: {error:?}"))?
            .ok_or_else(|| "personal API key did not authenticate".to_string())?;
        require(
            auth.actor_user_id.is_none() && auth.credential_owner_user_id == Some(user_id),
            "personal API key attribution did not separate its credential owner".to_string(),
        )?;
        let reservation = PostgresUsageStore::new(database.setup_pool.clone())
            .reserve(UsageCharge {
                tenant_id: auth.tenant_id.clone(),
                attribution: Some(auth.attribution()),
                request_id: format!("personal-key-{}", Uuid::new_v4()),
                admission_session_id: None,
                operation: "generation",
                provider_id: "openai-codex".to_string(),
                model: "gpt-image-2".to_string(),
                output_count: 1,
                billable_units: 1,
                billing_metric: BillingMetric::Output,
                limits: UsageLimits {
                    five_hour_image_limit: 100,
                    seven_day_image_limit: 100,
                },
            })
            .await
            .map_err(|error| format!("personal API key usage reserve failed: {error:?}"))?;
        let frozen_owner: (String, Option<Uuid>, Option<Uuid>) = sqlx::query_as(
            r#"
            SELECT auth_kind, actor_user_id, credential_owner_user_id
            FROM job_auth_attributions
            WHERE job_id = $1
            "#,
        )
        .bind(reservation.job_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect personal key attribution: {error}"))?;
        require(
            frozen_owner == ("api_key".to_string(), None, Some(user_id)),
            format!("personal key attribution was not frozen correctly: {frozen_owner:?}"),
        )?;
        auth.require_api_key_capability(ApiKeyCapability::ModelsRead)
            .map_err(|error| format!("models read was unexpectedly denied: {error:?}"))?;
        auth.require_api_key_capability(ApiKeyCapability::ImagesWrite)
            .map_err(|error| format!("images write was unexpectedly denied: {error:?}"))?;
        require(
            auth.require_api_key_capability(ApiKeyCapability::VideosRead)
                .is_err(),
            "videos read was unexpectedly permitted".to_string(),
        )?;
        let keys = store
            .list_project_api_keys_for_user(&project_id, user_id, None, 20)
            .await
            .map_err(|error| format!("failed to list personal API keys: {error:?}"))?;
        require(
            keys.data.len() == 1 && keys.data[0].owner.owner_type == "user",
            "personal API key was not listed as user-owned".to_string(),
        )?;
        let audit_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM identity_audit_events WHERE action = 'project.api_key.create' AND resource_id = $1",
        )
        .bind(&created.id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect API key audit event: {error}"))?;
        require(
            audit_count == 1,
            "personal API key creation audit event was missing".to_string(),
        )?;
        let updated_permissions = ApiKeyPermissions(BTreeMap::from([
            ("models".to_string(), ApiKeyPermissionLevel::Read),
            ("images".to_string(), ApiKeyPermissionLevel::Read),
            ("videos".to_string(), ApiKeyPermissionLevel::Read),
        ]));
        let updated = store
            .update_project_api_key(
                &project_id,
                &created.id,
                user_id,
                false,
                "Read-only media",
                ApiKeyPermissionMode::Restricted,
                updated_permissions,
            )
            .await
            .map_err(|error| format!("failed to update personal API key: {error:?}"))?;
        require(
            updated.name == "Read-only media",
            "personal API key update did not persist its name".to_string(),
        )?;
        let updated_auth = store
            .authenticate(&created.value)
            .await
            .map_err(|error| format!("failed to authenticate updated API key: {error:?}"))?
            .ok_or_else(|| "updated API key did not authenticate".to_string())?;
        updated_auth
            .require_api_key_capability(ApiKeyCapability::VideosRead)
            .map_err(|error| format!("updated videos read was denied: {error:?}"))?;
        require(
            updated_auth
                .require_api_key_capability(ApiKeyCapability::ImagesWrite)
                .is_err(),
            "updated images write was unexpectedly permitted".to_string(),
        )?;
        let rotated = store
            .rotate_project_api_key(&project_id, &created.id, user_id, false)
            .await
            .map_err(|error| format!("failed to rotate personal API key: {error:?}"))?;
        require(
            store
                .authenticate(&created.value)
                .await
                .map_err(|error| format!("failed to reject replaced API key: {error:?}"))?
                .is_none(),
            "replaced API key still authenticated after rotation".to_string(),
        )?;
        require(
            store
                .authenticate(&rotated.api_key.value)
                .await
                .map_err(|error| format!("failed to authenticate rotated API key: {error:?}"))?
                .is_some(),
            "rotated API key did not authenticate".to_string(),
        )?;
        let lifecycle_audits: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM identity_audit_events
            WHERE actor_user_id = $1
              AND action IN ('project.api_key.create',
                             'project.api_key.update',
                             'project.api_key.rotate')
              AND (
                resource_id = $2
                OR resource_id = $3
              )
            "#,
        )
        .bind(user_id)
        .bind(&created.id)
        .bind(&rotated.api_key.id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect API key lifecycle audits: {error}"))?;
        require(
            lifecycle_audits == 3,
            "personal API key lifecycle audit events were incomplete".to_string(),
        )?;
        sqlx::query(
            "UPDATE identity_project_memberships SET state = 'disabled' WHERE project_id = $1 AND user_id = $2",
        )
        .bind(&project_id)
        .bind(user_id)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to disable project membership: {error}"))?;
        let listed_after_removal = store
            .list_project_api_keys(&project_id, None, 20)
            .await
            .map_err(|error| format!("failed to list key after membership removal: {error:?}"))?;
        require(
            listed_after_removal.data.len() == 1
                && listed_after_removal.data[0].owner_project_access == "inactive"
                && listed_after_removal.data[0].status == "owner_access_lost",
            "removed member API key did not expose lost owner access".to_string(),
        )?;
        require(
            store
                .authenticate(&rotated.api_key.value)
                .await
                .map_err(|error| format!("membership-bound authentication failed: {error:?}"))?
                .is_none(),
            "personal API key remained active after membership was disabled".to_string(),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn disabling_user_api_keys_blocks_creation_and_authentication_only_for_users() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let project_id = format!("proj_{}", Uuid::new_v4().simple());
        let owner_user_id = Uuid::new_v4();
        insert_project(&database.setup_pool, &project_id).await?;
        insert_project_member(&database.setup_pool, &project_id, owner_user_id).await?;
        let store = PostgresApiKeyStore::new(
            database.pool("disable_user_keys").await?,
            test_keyring(),
        );
        let user_key = store
            .create_user_api_key(
                &project_id,
                owner_user_id,
                "Project owner",
                "owner@disable-user-keys.test",
                "Owner key",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(|error| format!("failed to create initial user key: {error:?}"))?;
        let service_key = store
            .create_service_account_for_actor(
                &project_id,
                "Deployment bot",
                owner_user_id,
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(|error| format!("failed to create initial service key: {error:?}"))?;
        let initial = store
            .get_project(&project_id)
            .await
            .map_err(|error| format!("failed to read project settings: {error:?}"))?;
        let disabled = store
            .update_project_settings(
                &project_id,
                owner_user_id,
                "Disabled personal keys",
                ProjectServiceTier::Priority,
                true,
                initial.settings_version,
            )
            .await
            .map_err(|error| format!("failed to disable user keys: {error:?}"))?;
        require(
            disabled.user_api_keys_disabled
                && disabled.service_tier == ProjectServiceTier::Priority
                && disabled.name == "Disabled personal keys"
                && disabled.settings_version == initial.settings_version + 1,
            "project general settings were not versioned atomically".to_string(),
        )?;
        require(
            store
                .authenticate(&user_key.value)
                .await
                .map_err(|error| format!("failed to reject disabled user key: {error:?}"))?
                .is_none(),
            "existing user key authenticated while project user keys were disabled".to_string(),
        )?;
        let service_auth = store
            .authenticate(&service_key.api_key.value)
            .await
            .map_err(|error| format!("service key authentication failed: {error:?}"))?;
        require(
            service_auth
                .as_ref()
                .is_some_and(|auth| auth.project_service_tier == ProjectServiceTier::Priority),
            "service-account authentication did not carry the project service tier".to_string(),
        )?;
        let creation_error = store
            .create_user_api_key(
                &project_id,
                owner_user_id,
                "Project owner",
                "owner@disable-user-keys.test",
                "Blocked key",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .expect_err("user-key creation unexpectedly succeeded while disabled");
        require(
            creation_error.status_code().as_u16() == 403,
            format!("disabled user-key creation returned the wrong error: {creation_error:?}"),
        )?;
        let service_key_while_disabled = store
            .create_service_account_for_actor(
                &project_id,
                "Second deployment bot",
                owner_user_id,
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(|error| {
                format!("service-key creation was blocked by user-key policy: {error:?}")
            })?;
        require(
            store
                .authenticate(&service_key_while_disabled.api_key.value)
                .await
                .map_err(|error| format!("new service key authentication failed: {error:?}"))?
                .is_some(),
            "new service-account key did not authenticate while user keys were disabled".to_string(),
        )?;
        let listed = store
            .list_project_api_keys_for_user(&project_id, owner_user_id, None, 20)
            .await
            .map_err(|error| format!("failed to list disabled user key: {error:?}"))?;
        require(
            listed.data.len() == 1
                && listed.data[0].status == "project_user_keys_disabled"
                && listed.data[0].owner_project_access == "active",
            "disabled user key was not surfaced as a project policy state".to_string(),
        )?;
        let conflict = store
            .update_project_settings(
                &project_id,
                owner_user_id,
                "Stale update",
                ProjectServiceTier::Default,
                false,
                initial.settings_version,
            )
            .await
            .expect_err("stale project settings update unexpectedly succeeded");
        require(
            conflict.status_code().as_u16() == 409,
            format!("stale project settings update returned the wrong error: {conflict:?}"),
        )?;
        let enabled = store
            .update_project_settings(
                &project_id,
                owner_user_id,
                "Enabled personal keys",
                ProjectServiceTier::Default,
                false,
                disabled.settings_version,
            )
            .await
            .map_err(|error| format!("failed to re-enable user keys: {error:?}"))?;
        require(
            !enabled.user_api_keys_disabled
                && store
                    .authenticate(&user_key.value)
                    .await
                    .map_err(|error| {
                        format!("failed to authenticate re-enabled user key: {error:?}")
                    })?
                    .is_some(),
            "existing user key did not recover when the project policy was re-enabled".to_string(),
        )?;
        let audit_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM identity_audit_events WHERE actor_user_id = $1 AND action = 'project.settings.update' AND resource_id = $2",
        )
        .bind(owner_user_id)
        .bind(&project_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect project settings audits: {error}"))?;
        require(
            audit_count == 2,
            "project settings updates were not audited exactly once".to_string(),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn pepper_rotation_preserves_only_configured_versions() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = pepper_rotation_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn legacy_sha_key_authenticates_during_migration_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = legacy_key_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn concurrent_authentication_does_not_deadlock_on_last_used_update() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = concurrent_authentication_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn routed_service_account_creation_is_atomic_and_visible_in_key_listing() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let project_id = format!("proj_{}", Uuid::new_v4().simple());
        insert_project(&database.setup_pool, &project_id).await?;
        let account_route_id = insert_codex_route(&database.setup_pool).await?;
        let route_id = insert_codex_group_route_with_policy(
            &database.setup_pool,
            &[(account_route_id, 0, 100, 0)],
            "quota_aware_least_loaded",
            "allow",
        )
        .await?;
        let store = PostgresApiKeyStore::new(database.pool("routed").await?, test_keyring());
        require(
            store
                .create_service_account_with_route(
                    &project_id,
                    "Direct account route",
                    account_route_id,
                    ApiKeyPermissionMode::All,
                    ApiKeyPermissions::default(),
                )
                .await
                .is_err(),
            "user-facing API key creation accepted a direct account route".to_string(),
        )?;
        let created = store
            .create_service_account_with_route(
                &project_id,
                "Routed key",
                route_id,
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(|error| format!("failed to create routed API key: {error:?}"))?;

        let binding: (Uuid, i64, String, String) = sqlx::query_as(
            r#"
            SELECT route_id, route_revision, provider_id, operation_id
            FROM gateway_api_key_provider_routes
            WHERE api_key_id = $1
            "#,
        )
        .bind(&created.api_key.id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read routed API key binding: {error}"))?;
        require(
            binding
                == (
                    route_id,
                    1,
                    "openai-codex".to_string(),
                    "images.generations".to_string(),
                ),
            format!("unexpected routed API key binding: {binding:?}"),
        )?;

        let listed = store
            .list_project_api_keys(&project_id, None, 20)
            .await
            .map_err(|error| format!("failed to list routed API key: {error:?}"))?;
        require(
            listed.data.len() == 1
                && listed.data[0].provider_routes.len() == 1
                && listed.data[0].provider_routes[0].route_id == route_id.to_string()
                && listed.data[0].provider_routes[0].route_revision == 1
                && listed.data[0].provider_routes[0].model_count == 1,
            format!(
                "routed API key was not projected in listing: {:?}",
                listed.data
            ),
        )?;
        let authz_version: i64 =
            sqlx::query_scalar("SELECT authz_version FROM gateway_api_keys WHERE id = $1")
                .bind(&created.api_key.id)
                .fetch_one(&database.setup_pool)
                .await
                .map_err(|error| {
                    format!("failed to read API key authorization version: {error}")
                })?;
        let model_routing = PostgresModelRoutingStore::new(database.pool("model_routing").await?);
        let models = model_routing
            .list_api_key_models(&project_id, &created.api_key.id, authz_version)
            .await
            .map_err(|error| format!("failed to list routed models: {error:?}"))?;
        require(
            models.len() == 1 && models[0].id == "gpt-image-2",
            format!("unexpected routed model list: {models:?}"),
        )?;
        let resolved = model_routing
            .resolve_api_key_model(
                &project_id,
                &created.api_key.id,
                authz_version,
                "openai-codex",
                "images.generations",
                "openai-images-v1",
                Some("gpt-image-2"),
                "gpt-image-2",
            )
            .await
            .map_err(|error| format!("failed to resolve routed model: {error:?}"))?
            .ok_or_else(|| "routed API key unexpectedly fell back to legacy routing".to_owned())?;
        require(
            resolved.execution_model_id == "gpt-image-2" && resolved.route_revision == 1,
            format!("unexpected routed model resolution: {resolved:?}"),
        )?;
        let surface = model_routing
            .resolve_api_key_surface_model(
                &project_id,
                &created.api_key.id,
                authz_version,
                "images.generations",
                &["openai-images-v1".to_owned(), "xai-images-v1".to_owned()],
                "gpt-image-2",
            )
            .await
            .map_err(|error| format!("failed to resolve image surface model: {error:?}"))?
            .ok_or_else(|| "image surface unexpectedly fell back to legacy routing".to_owned())?;
        require(
            surface.provider_id == "openai-codex"
                && surface.api_profile == "openai-images-v1"
                && surface.route_revision == 1,
            format!("unexpected image surface resolution: {surface:?}"),
        )?;
        let routed_job_id = Uuid::new_v4();
        let routed_request_id = format!("routed-key-{}", Uuid::new_v4());
        sqlx::query(
            r#"
            INSERT INTO jobs (
                job_id, tenant_id, request_id, operation, provider_id, model,
                state, requested_units, output_count, billable_units,
                billing_metric, billing_unit, charged_units,
                created_at_ms, updated_at_ms
            )
            VALUES (
                $1, $2, $3, 'generation', 'openai-codex', 'gpt-image-2',
                'failed', 1, 1, 1, 'output', 'output', 0, 1, 1
            )
            "#,
        )
        .bind(routed_job_id)
        .bind(&project_id)
        .bind(&routed_request_id)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to seed routed key job: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO job_auth_attributions (
                job_id, tenant_id, project_id, service_account_id, api_key_id,
                credential_authz_version, credential_owner_user_id,
                actor_user_id, actor_session_id, actor_authz_version,
                route_provider_id, route_operation_id, route_command_schema,
                route_id, route_revision, auth_kind, admitted_at_ms
            )
            VALUES (
                $1, $2, $2, $3, $4, $5, NULL,
                NULL, NULL, NULL,
                $6, $7, $8, $9, $10, 'api_key', 1
            )
            "#,
        )
        .bind(routed_job_id)
        .bind(&project_id)
        .bind(&created.id)
        .bind(&created.api_key.id)
        .bind(authz_version)
        .bind(&surface.provider_id)
        .bind(&surface.operation_id)
        .bind(&surface.command_schema)
        .bind(surface.route_id)
        .bind(surface.route_revision)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to freeze routed key attribution: {error}"))?;
        let frozen_route: (String, Option<Uuid>, Option<i64>) = sqlx::query_as(
            r#"
            SELECT auth_kind, route_id, route_revision
            FROM job_auth_attributions
            WHERE job_id = $1
            "#,
        )
        .bind(routed_job_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect routed key attribution: {error}"))?;
        require(
            frozen_route == ("api_key".to_string(), Some(route_id), Some(1)),
            format!("routed key attribution was not frozen correctly: {frozen_route:?}"),
        )?;
        let policy_actor = Uuid::new_v4();
        let now_ms: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read policy clock: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO identity_users(
                user_id, normalized_email, display_name, roles, scopes,
                created_at_ms, updated_at_ms
            )
            VALUES (
                $1, $2, 'Project policy actor',
                ARRAY['platform_owner'], ARRAY['admin:*'], $3, $3
            )
            "#,
        )
        .bind(policy_actor)
        .bind(format!("policy-{}@limits.test", policy_actor.simple()))
        .bind(now_ms)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to insert project policy actor: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO project_model_policies(
                project_id, organization_id, created_by_user_id,
                updated_by_user_id, created_at_ms, updated_at_ms
            )
            SELECT id, tenant_id, $2, $2, $3, $3
            FROM gateway_projects
            WHERE id = $1
            "#,
        )
        .bind(&project_id)
        .bind(policy_actor)
        .bind(now_ms)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to configure deny-all project policy: {error}"))?;

        let denied_models = model_routing
            .list_api_key_models(&project_id, &created.api_key.id, authz_version)
            .await
            .map_err(|error| format!("failed to list policy-filtered models: {error:?}"))?;
        require(
            denied_models.is_empty(),
            format!("project model policy leaked models to service key: {denied_models:?}"),
        )?;
        let denied_surface = model_routing
            .resolve_api_key_surface_model(
                &project_id,
                &created.api_key.id,
                authz_version,
                "images.generations",
                &["openai-images-v1".to_owned()],
                "gpt-image-2",
            )
            .await
            .expect_err("service key bypassed the project model deny-list");
        require(
            denied_surface.status_code().as_u16() == 404,
            format!(
                "project model deny-list returned {} to service key",
                denied_surface.status_code()
            ),
        )?;
        sqlx::query(
            r#"
            INSERT INTO project_model_access_entries(
                project_id, operation_id, api_profile, public_model_id,
                media_kind, created_at_ms
            )
            VALUES ($1, 'images.generations', 'openai-images-v1',
                    'gpt-image-2', 'image', $2)
            "#,
        )
        .bind(&project_id)
        .bind(now_ms)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to allow project model: {error}"))?;
        let restored_surface = model_routing
            .resolve_api_key_surface_model(
                &project_id,
                &created.api_key.id,
                authz_version,
                "images.generations",
                &["openai-images-v1".to_owned()],
                "gpt-image-2",
            )
            .await
            .map_err(|error| format!("allowed project model failed to resolve: {error:?}"))?
            .ok_or_else(|| "allowed project model unexpectedly used legacy routing".to_owned())?;
        require(
            restored_surface.public_model_id == "gpt-image-2",
            format!("unexpected restored model route: {restored_surface:?}"),
        )?;
        let cross_operation = model_routing
            .resolve_api_key_model(
                &project_id,
                &created.api_key.id,
                authz_version,
                "grok-cli",
                "videos.generations",
                "xai-videos-v1",
                Some("grok-imagine-video"),
                "grok-imagine-video",
            )
            .await
            .expect_err("routed image key unexpectedly fell back to video routing");
        require(
            cross_operation.status_code().as_u16() == 404,
            format!(
                "cross-operation routed key returned {}",
                cross_operation.status_code()
            ),
        )?;

        let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM gateway_service_accounts")
            .fetch_one(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to count service accounts: {error}"))?;
        require(
            store
                .create_service_account_with_route(
                    &project_id,
                    "Missing route",
                    Uuid::new_v4(),
                    ApiKeyPermissionMode::All,
                    ApiKeyPermissions::default(),
                )
                .await
                .is_err(),
            "missing route unexpectedly created an API key".to_string(),
        )?;
        let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM gateway_service_accounts")
            .fetch_one(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to recount service accounts: {error}"))?;
        require(
            before == after,
            "failed routed creation left a service account behind".to_string(),
        )?;
        sqlx::query("UPDATE gateway_api_keys SET authz_version = authz_version + 1 WHERE id = $1")
            .bind(&created.api_key.id)
            .execute(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to rotate API key authorization version: {error}"))?;
        let stale = model_routing
            .resolve_api_key_surface_model(
                &project_id,
                &created.api_key.id,
                authz_version,
                "images.generations",
                &["openai-images-v1".to_owned(), "xai-images-v1".to_owned()],
                "gpt-image-2",
            )
            .await
            .expect_err("stale authorization version unexpectedly resolved a model");
        require(
            stale.status_code().as_u16() == 401,
            format!(
                "stale authorization version returned {}",
                stale.status_code()
            ),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn replacement_profiles_advance_bound_routes_without_losing_catalog_models() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let project_id = format!("proj_{}", Uuid::new_v4().simple());
        insert_project(&database.setup_pool, &project_id).await?;
        let image_account_route = insert_codex_route(&database.setup_pool).await?;
        let image_route = insert_codex_group_route_with_policy(
            &database.setup_pool,
            &[(image_account_route, 3, 70, 15)],
            "quota_aware_least_loaded",
            "allow",
        )
        .await?;
        let (video_route, video_model_id) =
            insert_test_video_route(&database.setup_pool, image_account_route).await?;
        let store = PostgresApiKeyStore::new(database.pool("profile_upgrade").await?, test_keyring());
        let permissions = ApiKeyPermissions(BTreeMap::from([
            ("models".to_string(), ApiKeyPermissionLevel::Read),
            ("images".to_string(), ApiKeyPermissionLevel::Write),
            ("videos".to_string(), ApiKeyPermissionLevel::Write),
        ]));
        let created = store
            .create_service_account_with_route(
                &project_id,
                "Profile upgrade",
                image_route,
                ApiKeyPermissionMode::Restricted,
                permissions,
            )
            .await
            .map_err(|error| format!("failed to create routed API key: {error:?}"))?;
        bind_test_api_key_route(
            &database.setup_pool,
            &project_id,
            &created.api_key.id,
            video_route,
        )
        .await?;
        bind_test_console_routes(
            &database.setup_pool,
            &project_id,
            image_route,
            video_route,
        )
        .await?;

        let initial_report = reconcile_execution_profile_routes(&database.setup_pool)
            .await
            .map_err(|error| format!("new-install reconciliation failed: {error:?}"))?;
        require(
            initial_report.inspected_routes == 0 && initial_report.revised_routes == 0,
            format!("healthy new routes were unexpectedly revised: {initial_report:?}"),
        )?;
        let model_routing = PostgresModelRoutingStore::new(database.pool("upgrade_models").await?);
        let initial_authz_version: i64 = sqlx::query_scalar(
            "SELECT authz_version FROM gateway_api_keys WHERE id = $1",
        )
        .bind(&created.api_key.id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read initial authz version: {error}"))?;
        let initial_models = model_routing
            .list_api_key_models(
                &project_id,
                &created.api_key.id,
                initial_authz_version,
            )
            .await
            .map_err(|error| format!("failed to list initial models: {error:?}"))?;
        require(
            initial_models.iter().any(|model| model.id == "gpt-image-2")
                && initial_models.iter().any(|model| model.id == video_model_id),
            format!("initial image/video catalog was incomplete: {initial_models:?}"),
        )?;
        insert_legacy_codex_snapshot_mappings(
            &database.setup_pool,
            &[image_account_route, image_route],
        )
        .await?;

        let replacements = replace_current_route_profiles(
            &database.setup_pool,
            &[image_account_route, image_route, video_route],
        )
        .await?;
        let empty_models = model_routing
            .list_api_key_models(
                &project_id,
                &created.api_key.id,
                initial_authz_version,
            )
            .await
            .map_err(|error| format!("failed to observe stale catalog: {error:?}"))?;
        require(
            empty_models.is_empty(),
            format!("disabled legacy profiles remained visible: {empty_models:?}"),
        )?;

        let report = reconcile_execution_profile_routes(&database.setup_pool)
            .await
            .map_err(|error| format!("historical route reconciliation failed: {error:?}"))?;
        require(
            report.inspected_routes == 3
                && report.revised_routes == 3
                && report.unresolved_routes == 0
                && report.api_key_bindings_moved == 2
                && report.project_bindings_moved == 1
                && report.platform_bindings_moved == 1,
            format!("unexpected route reconciliation report: {report:?}"),
        )?;
        let current_authz_version: i64 = sqlx::query_scalar(
            "SELECT authz_version FROM gateway_api_keys WHERE id = $1",
        )
        .bind(&created.api_key.id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read reconciled authz version: {error}"))?;
        require(
            current_authz_version == initial_authz_version + 2,
            format!(
                "route binding migration did not invalidate authorization snapshots: {initial_authz_version} -> {current_authz_version}"
            ),
        )?;
        let models = model_routing
            .list_api_key_models(
                &project_id,
                &created.api_key.id,
                current_authz_version,
            )
            .await
            .map_err(|error| format!("failed to list reconciled models: {error:?}"))?;
        require(
            models.iter().any(|model| model.id == "gpt-image-2")
                && models
                    .iter()
                    .any(|model| model.id == "gpt-image-2-2026-04-21")
                && models.iter().any(|model| model.id == video_model_id),
            format!("reconciled image/video catalog was incomplete: {models:?}"),
        )?;
        assert_codex_snapshot_mapping_reconciled(
            &database.setup_pool,
            &[image_account_route, image_route],
        )
        .await?;
        assert_reconciled_route_copies(
            &database.setup_pool,
            &[image_account_route, image_route, video_route],
        )
        .await?;

        let repeated = reconcile_execution_profile_routes(&database.setup_pool)
            .await
            .map_err(|error| format!("repeated reconciliation failed: {error:?}"))?;
        require(
            repeated.inspected_routes == 0 && repeated.revised_routes == 0,
            format!("repeated reconciliation was not idempotent: {repeated:?}"),
        )?;
        let revision_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_routes WHERE route_id = ANY($1)",
        )
        .bind(vec![image_account_route, image_route, video_route])
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to count route revisions: {error}"))?;
        require(
            revision_count == 6,
            format!("repeated reconciliation created duplicate revisions: {revision_count}"),
        )?;

        let (_, manually_disabled_profile) = replacements
            .iter()
            .find(|(route_id, _)| *route_id == video_route)
            .copied()
            .ok_or_else(|| "video replacement profile was not recorded".to_string())?;
        sqlx::query(
            "UPDATE provider_execution_profiles SET state = 'disabled', updated_at_ms = updated_at_ms + 1 WHERE execution_profile_id = $1",
        )
        .bind(manually_disabled_profile)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to manually disable replacement profile: {error}"))?;
        let disabled_report = reconcile_execution_profile_routes(&database.setup_pool)
            .await
            .map_err(|error| format!("manual-disable reconciliation failed: {error:?}"))?;
        require(
            disabled_report.revised_routes == 0 && disabled_report.unresolved_routes == 1,
            format!("manual disable unexpectedly rewrote a route: {disabled_report:?}"),
        )?;
        let disabled_state: (String, i64, i64) = sqlx::query_as(
            r#"
            SELECT profile.state, head.current_revision, COUNT(route.revision)
            FROM provider_execution_profiles profile
            CROSS JOIN provider_route_heads head
            JOIN provider_routes route ON route.route_id = head.route_id
            WHERE profile.execution_profile_id = $1 AND head.route_id = $2
            GROUP BY profile.state, head.current_revision
            "#,
        )
        .bind(manually_disabled_profile)
        .bind(video_route)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect manual disable boundary: {error}"))?;
        require(
            disabled_state == ("disabled".to_string(), 2, 2),
            format!("manual disable was not preserved: {disabled_state:?}"),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn disabled_route_fails_closed_during_job_attachment() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let project_id = format!("proj_{}", Uuid::new_v4().simple());
        insert_project(&database.setup_pool, &project_id).await?;
        let account_route_id = insert_codex_route(&database.setup_pool).await?;
        let route_id = insert_codex_group_route_with_policy(
            &database.setup_pool,
            &[(account_route_id, 0, 100, 0)],
            "quota_aware_least_loaded",
            "allow",
        )
        .await?;
        let key_store =
            PostgresApiKeyStore::new(database.pool("disabled_route_key").await?, test_keyring());
        let created = key_store
            .create_service_account_with_route(
                &project_id,
                "Disabled route",
                route_id,
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(|error| format!("failed to create routed API key: {error:?}"))?;

        let request_id = format!("request-{}", Uuid::new_v4().simple());
        let request_hash = "d".repeat(64);
        let admission =
            PostgresAdmissionStore::new(database.pool("disabled_route_attach").await?);
        let ticket = match admission
            .claim(ClaimAdmission {
                owner_token: Uuid::new_v4(),
                tenant_id: project_id.clone(),
                project_id: project_id.clone(),
                api_profile: "openai-images-v1".to_string(),
                operation: "generation".to_string(),
                request_id: request_id.clone(),
                idempotency_key_digest: None,
                request_hash: request_hash.clone(),
                deadline_at_ms: i64::MAX,
            })
            .await
            .map_err(|error| format!("failed to claim admission: {error:?}"))?
        {
            AdmissionClaim::Owner(ticket) => ticket,
            other => return Err(format!("unexpected admission claim: {other:?}")),
        };

        let job_id = Uuid::new_v4();
        let reservation_id = Uuid::new_v4();
        let now: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read database clock: {error}"))?;
        let mut tx = database
            .setup_pool
            .begin()
            .await
            .map_err(|error| format!("failed to begin routed job seed: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO jobs
              (job_id, tenant_id, request_id, operation, provider_id, model, state,
               requested_units, output_count, billable_units, billing_metric, billing_unit,
               charged_units, reservation_id, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, 'generation', 'openai-codex', 'gpt-image-2',
                    'reserved', 1, 1, 1, 'output', 'output', 0, $4, $5, $5)
            "#,
        )
        .bind(job_id)
        .bind(&project_id)
        .bind(&request_id)
        .bind(reservation_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("failed to insert routed job: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO quota_reservations
              (reservation_id, tenant_id, request_id, job_id, requested_units,
               committed_units, started_units, released_units, state,
               created_at_ms, updated_at_ms, expires_at_ms,
               limit_5h, remaining_5h, limit_7d, remaining_7d,
               billing_metric, billing_unit)
            VALUES ($1, $2, $3, $4, 1, 0, 0, 0, 'reserved', $5, $5,
                    9223372036854775807, 100, 99, 100, 99, 'output', 'output')
            "#,
        )
        .bind(reservation_id)
        .bind(&project_id)
        .bind(&request_id)
        .bind(job_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("failed to insert routed quota reservation: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO job_auth_attributions
              (job_id, tenant_id, project_id, service_account_id, api_key_id,
               credential_authz_version, auth_kind, admitted_at_ms)
            VALUES ($1, $2, $2, $3, $4, 1, 'api_key', $5)
            "#,
        )
        .bind(job_id)
        .bind(&project_id)
        .bind(&created.id)
        .bind(&created.api_key.id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("failed to insert routed auth attribution: {error}"))?;
        tx.commit()
            .await
            .map_err(|error| format!("failed to commit routed job seed: {error}"))?;

        sqlx::query(
            "UPDATE provider_route_heads SET state = 'disabled', updated_at_ms = $2 WHERE route_id = $1",
        )
        .bind(route_id)
        .bind(now + 1)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to disable provider route: {error}"))?;

        let attach = admission
            .attach(AttachJob {
                ticket,
                job_id,
                command_schema: "openai.images.generation.v1".to_string(),
                command_json: json!({"prompt": "must not bypass a disabled route"}),
                input_manifest: None,
                work_kind: "image_batch".to_string(),
                schedule_scope: project_id.clone(),
                schedule_weight: 1,
                schedule_priority: 1,
                schedule_cost: 1,
                contract: AdmissionContract::LegacyV1,
                customer_pricing: None,
            })
            .await;
        require(
            matches!(attach, Err(AdmissionError::InvalidCommand)),
            format!("disabled provider route did not fail closed: {attach:?}"),
        )?;
        let persisted: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM job_payloads WHERE job_id = $1),
              (SELECT COUNT(*) FROM job_provider_route_attributions WHERE job_id = $1),
              (SELECT COUNT(*) FROM work_items WHERE job_id = $1)
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect disabled route rollback: {error}"))?;
        require(
            persisted == (0, 0, 0),
            format!("disabled route attach left partial state: {persisted:?}"),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn routed_work_enforces_quota_pressure_reserve_and_unknown_policy() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let project_id = format!("proj_{}", Uuid::new_v4().simple());
        insert_project(&database.setup_pool, &project_id).await?;
        let high_quota_route_id = insert_codex_route(&database.setup_pool).await?;
        let low_quota_route_id = insert_codex_route(&database.setup_pool).await?;
        let high_quota_profile_id: Uuid = sqlx::query_scalar(
            "SELECT execution_profile_id FROM provider_route_members WHERE route_id = $1",
        )
        .bind(high_quota_route_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read allowed profile: {error}"))?;
        let low_quota_profile_id: Uuid = sqlx::query_scalar(
            "SELECT execution_profile_id FROM provider_route_members WHERE route_id = $1",
        )
        .bind(low_quota_route_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read other profile: {error}"))?;
        let route_id = insert_codex_group_route_with_policy(
            &database.setup_pool,
            &[
                (high_quota_route_id, 0, 100, 20),
                (low_quota_route_id, 0, 100, 0),
            ],
            "quota_aware_least_loaded",
            "block",
        )
        .await?;
        let store = PostgresApiKeyStore::new(database.pool("route_claim_key").await?, test_keyring());
        let created = store
            .create_service_account_with_route(
                &project_id,
                "Route claim",
                route_id,
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(|error| format!("failed to create routed API key: {error:?}"))?;
        let (tenant_id, service_account_id): (String, String) = sqlx::query_as(
            "SELECT tenant_id, service_account_id FROM gateway_api_keys WHERE id = $1",
        )
        .bind(&created.api_key.id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read API key owner: {error}"))?;
        let job_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let work_item_id = Uuid::new_v4();
        let now: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read database clock: {error}"))?;
        let request_id = format!("request-{}", Uuid::new_v4().simple());
        for (profile_id, used_percent) in [
            (high_quota_profile_id, 70_i32),
            (low_quota_profile_id, 15_i32),
        ] {
            let provider_account_id: Uuid = sqlx::query_scalar(
                "SELECT provider_account_id FROM provider_execution_profiles WHERE execution_profile_id = $1",
            )
            .bind(profile_id)
            .fetch_one(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to read quota account: {error}"))?;
            sqlx::query(
                "INSERT INTO provider_account_quota_snapshots (provider_account_id, provider_id, status, observed_at_ms) VALUES ($1, 'openai-codex', 'observed', $2)",
            )
            .bind(provider_account_id)
            .bind(now)
            .execute(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to insert quota snapshot: {error}"))?;
            sqlx::query(
                "INSERT INTO provider_account_quota_windows (provider_account_id, provider_id, limit_id, window_role, window_duration_mins, used_percent, resets_at_ms, observed_at_ms) VALUES ($1, 'openai-codex', 'codex', 'primary', 300, $2, $3, $4)",
            )
            .bind(provider_account_id)
            .bind(used_percent)
            .bind(now + 300_000)
            .bind(now)
            .execute(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to insert quota window: {error}"))?;
        }
        sqlx::query(
            "INSERT INTO jobs (job_id, tenant_id, request_id, operation, provider_id, model, state, requested_units, output_count, billable_units, billing_metric, billing_unit, economics_contract_version, created_at_ms, updated_at_ms) VALUES ($1, $2, $3, 'generation', 'openai-codex', 'gpt-image-2', 'reserved', 1, 1, 1, 'output', 'output', 2, $4, $4)",
        )
        .bind(job_id)
        .bind(&tenant_id)
        .bind(&request_id)
        .bind(now)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to insert routed job: {error}"))?;
        sqlx::query(
            "INSERT INTO job_auth_attributions (job_id, tenant_id, project_id, service_account_id, api_key_id, credential_authz_version, auth_kind, admitted_at_ms) VALUES ($1, $2, $3, $4, $5, 1, 'api_key', $6)",
        )
        .bind(job_id)
        .bind(&tenant_id)
        .bind(&project_id)
        .bind(&service_account_id)
        .bind(&created.api_key.id)
        .bind(now)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to insert job auth attribution: {error}"))?;
        sqlx::query(
            "INSERT INTO job_provider_route_attributions (job_id, tenant_id, api_key_id, provider_id, operation_id, command_schema, route_id, route_revision, attributed_at_ms) VALUES ($1, $2, $3, 'openai-codex', 'images.generations', 'openai.images.generation.v1', $4, 1, $5)",
        )
        .bind(job_id)
        .bind(&tenant_id)
        .bind(&created.api_key.id)
        .bind(route_id)
        .bind(now)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to insert route attribution: {error}"))?;
        sqlx::query(
            "INSERT INTO admission_sessions (session_id, owner_token, tenant_id, project_id, api_profile, operation, request_id, request_hash, state, job_id, deadline_at_ms, created_at_ms, updated_at_ms) VALUES ($1, $2, $3, $4, 'openai-images-v1', 'generation', $5, $6, 'attached', $7, $8, $9, $9)",
        )
        .bind(session_id)
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(&project_id)
        .bind(&request_id)
        .bind("c".repeat(64))
        .bind(job_id)
        .bind(now + 300_000)
        .bind(now)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to insert admission session: {error}"))?;
        sqlx::query(
            "INSERT INTO job_payloads (job_id, admission_session_id, command_schema, command_json, request_hash, created_at_ms) VALUES ($1, $2, 'openai.images.generation.v1', $3, $4, $5)",
        )
        .bind(job_id)
        .bind(session_id)
        .bind(serde_json::json!({"schema_version": 1, "operation": "generation", "n": 1}))
        .bind("c".repeat(64))
        .bind(now)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to insert job payload: {error}"))?;
        sqlx::query(
            "INSERT INTO work_items (work_item_id, job_id, kind, state, available_at_ms, created_at_ms, updated_at_ms) VALUES ($1, $2, 'generation', 'ready', $3, $3, $3)",
        )
        .bind(work_item_id)
        .bind(job_id)
        .bind(now)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to insert ready work: {error}"))?;

        sqlx::query(
            r#"
            INSERT INTO provider_routes
              (route_id, revision, route_key, display_name, provider_id, operation_id,
               command_schema, route_kind, selection_strategy, quota_freshness_ms,
               unknown_quota_policy, state, created_at_ms)
            SELECT route_id, 2, route_key, 'Codex quota group v2', provider_id,
                   operation_id, command_schema, route_kind, selection_strategy,
                   quota_freshness_ms, unknown_quota_policy, state, $2
            FROM provider_routes WHERE route_id = $1 AND revision = 1
            "#,
        )
        .bind(route_id)
        .bind(now + 1)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to insert provider route revision: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_route_members
              (route_id, route_revision, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, priority, weight, state,
               minimum_remaining_percent, created_at_ms)
            SELECT route_id, 2, provider_id, operation_id, command_schema,
                   provider_account_id, execution_profile_id, priority, weight, state,
                   minimum_remaining_percent, $2
            FROM provider_route_members WHERE route_id = $1 AND route_revision = 1
            "#,
        )
        .bind(route_id)
        .bind(now + 1)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to copy provider route members: {error}"))?;
        sqlx::query(
            "UPDATE provider_route_heads SET current_revision = 2, updated_at_ms = $2 WHERE route_id = $1",
        )
        .bind(route_id)
        .bind(now + 1)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to publish provider route revision: {error}"))?;
        let bound_revision: i64 = sqlx::query_scalar(
            "SELECT route_revision FROM gateway_api_key_provider_routes WHERE api_key_id = $1",
        )
        .bind(&created.api_key.id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read bound route revision: {error}"))?;
        require(
            bound_revision == 1,
            "route publication rewrote API key bindings".to_string(),
        )?;
        require(
            sqlx::query(
                "UPDATE provider_route_members SET weight = weight + 1 WHERE route_id = $1 AND route_revision = 1",
            )
            .bind(route_id)
            .execute(&database.setup_pool)
            .await
            .is_err(),
            "published route members remained mutable".to_string(),
        )?;

        let admission = PostgresAdmissionStore::new(database.pool("route_claim").await?);
        let legacy_rejected = admission
            .claim_ready(
                "legacy-worker",
                30_000,
                AdmissionContract::OutputEconomicsV2,
            )
            .await
            .map_err(|error| format!("legacy claim failed unexpectedly: {error:?}"))?;
        require(
            legacy_rejected.is_none(),
            "profile-less worker claimed routed work".to_string(),
        )?;
        let schema_rejected = admission
            .claim_ready_for_schema(
                "schema-worker",
                30_000,
                AdmissionContract::OutputEconomicsV2,
                "openai.images.generation.v1",
            )
            .await
            .map_err(|error| format!("schema claim failed unexpectedly: {error:?}"))?;
        require(
            schema_rejected.is_none(),
            "schema-only worker claimed routed work".to_string(),
        )?;
        let job_rejected = admission
            .claim_job(job_id, "job-worker", 30_000)
            .await
            .map_err(|error| format!("job claim failed unexpectedly: {error:?}"))?;
        require(
            job_rejected.is_none(),
            "profile-less targeted claim bypassed routed work".to_string(),
        )?;
        let rejected = admission
            .claim_ready_for_profile(
                "wrong-profile",
                30_000,
                AdmissionContract::OutputEconomicsV2,
                "openai.images.generation.v1",
                Uuid::new_v4(),
            )
            .await
            .map_err(|error| format!("wrong profile claim failed unexpectedly: {error:?}"))?;
        require(rejected.is_none(), "profile outside route claimed routed work".to_string())?;
        let quota_rejected = admission
            .claim_ready_for_profile(
                "high-quota-profile",
                30_000,
                AdmissionContract::OutputEconomicsV2,
                "openai.images.generation.v1",
                high_quota_profile_id,
            )
            .await
            .map_err(|error| format!("high quota profile claim failed unexpectedly: {error:?}"))?;
        require(
            quota_rejected.is_none(),
            "higher quota pressure profile won group selection".to_string(),
        )?;
        let high_quota_account_id: Uuid = sqlx::query_scalar(
            "SELECT provider_account_id FROM provider_execution_profiles WHERE execution_profile_id = $1",
        )
        .bind(high_quota_profile_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read high quota account: {error}"))?;
        sqlx::query(
            "UPDATE provider_account_quota_windows SET used_percent = 85 WHERE provider_account_id = $1",
        )
        .bind(high_quota_account_id)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to raise quota pressure: {error}"))?;
        let reserve_rejected = admission
            .claim_ready_for_profile(
                "reserved-profile",
                30_000,
                AdmissionContract::OutputEconomicsV2,
                "openai.images.generation.v1",
                high_quota_profile_id,
            )
            .await
            .map_err(|error| format!("reserved profile claim failed unexpectedly: {error:?}"))?;
        require(
            reserve_rejected.is_none(),
            "higher priority profile bypassed its minimum remaining quota".to_string(),
        )?;
        let low_quota_account_id: Uuid = sqlx::query_scalar(
            "SELECT provider_account_id FROM provider_execution_profiles WHERE execution_profile_id = $1",
        )
        .bind(low_quota_profile_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read low quota account: {error}"))?;
        sqlx::query(
            "UPDATE provider_account_quota_snapshots SET status = 'unavailable', last_error_code = 'quota_refresh_failed' WHERE provider_account_id = $1",
        )
        .bind(low_quota_account_id)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to mark quota unavailable: {error}"))?;
        let unknown_rejected = admission
            .claim_ready_for_profile(
                "unknown-quota-profile",
                30_000,
                AdmissionContract::OutputEconomicsV2,
                "openai.images.generation.v1",
                low_quota_profile_id,
            )
            .await
            .map_err(|error| format!("unknown quota claim failed unexpectedly: {error:?}"))?;
        require(
            unknown_rejected.is_none(),
            "failed quota refresh reused the old quota window".to_string(),
        )?;
        sqlx::query(
            "UPDATE provider_account_quota_snapshots SET status = 'observed', last_error_code = NULL WHERE provider_account_id = $1",
        )
        .bind(low_quota_account_id)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to restore observed quota: {error}"))?;
        sqlx::query(
            "INSERT INTO provider_account_model_configurations (provider_account_id, provider_id, mode, version, updated_at_ms) VALUES ($1, 'openai-codex', 'allowlist', 1, $2)",
        )
        .bind(low_quota_account_id)
        .bind(now)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to restrict account models: {error}"))?;
        let model_rejected = admission
            .claim_ready_for_profile(
                "model-restricted-profile",
                30_000,
                AdmissionContract::OutputEconomicsV2,
                "openai.images.generation.v1",
                low_quota_profile_id,
            )
            .await
            .map_err(|error| format!("model-restricted claim failed unexpectedly: {error:?}"))?;
        require(
            model_rejected.is_none(),
            "account claimed work outside its model allowlist".to_string(),
        )?;
        sqlx::query(
            r#"
            INSERT INTO provider_models
              (provider_id, model_id, execution_model_id, media_kind, display_name,
               adapter_state, lifecycle_state, operation_ids, source_kind,
               first_seen_at_ms, last_seen_at_ms, metadata_json)
            VALUES ('openai-codex', 'gpt-image-2', 'gpt-image-2', 'image',
                    'GPT Image 2', 'supported', 'enabled', ARRAY['images.generations'],
                    'adapter_contract', $1, $1, '{}'::JSONB)
            ON CONFLICT (provider_id, model_id, media_kind) DO NOTHING
            "#,
        )
        .bind(now)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to seed provider model: {error}"))?;
        sqlx::query(
            "INSERT INTO provider_account_model_bindings (provider_account_id, provider_id, model_id, media_kind, configured_at_ms) VALUES ($1, 'openai-codex', 'gpt-image-2', 'image', $2)",
        )
        .bind(low_quota_account_id)
        .bind(now)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to allow account model: {error}"))?;
        sqlx::query(
            "UPDATE provider_account_execution_controls SET lifecycle_state = 'draining', control_version = control_version + 1, drain_started_at_ms = $2, updated_at_ms = $2 WHERE provider_account_id = $1",
        )
        .bind(low_quota_account_id)
        .bind(now)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to drain low quota account: {error}"))?;
        let draining_rejected = admission
            .claim_ready_for_profile(
                "draining-unpinned-profile",
                30_000,
                AdmissionContract::OutputEconomicsV2,
                "openai.images.generation.v1",
                low_quota_profile_id,
            )
            .await
            .map_err(|error| format!("draining claim failed unexpectedly: {error:?}"))?;
        require(
            draining_rejected.is_none(),
            "draining account accepted new unpinned work".to_string(),
        )?;
        sqlx::query(
            "UPDATE work_items SET execution_profile_id = $2 WHERE work_item_id = $1",
        )
        .bind(work_item_id)
        .bind(low_quota_profile_id)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to pin work before drain retry: {error}"))?;
        let accepted = admission
            .claim_ready_for_profile(
                "draining-pinned-profile",
                30_000,
                AdmissionContract::OutputEconomicsV2,
                "openai.images.generation.v1",
                low_quota_profile_id,
            )
            .await
            .map_err(|error| format!("allowed profile claim failed: {error:?}"))?;
        require(
            accepted.as_ref().is_some_and(|lease| lease.job_id == job_id),
            "draining account did not recover work pinned before drain".to_string(),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn priority_weighted_rendezvous_tracks_configured_member_share() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let light_route_id = insert_codex_route(&database.setup_pool).await?;
        let heavy_route_id = insert_codex_route(&database.setup_pool).await?;
        let light_profile_id: Uuid = sqlx::query_scalar(
            "SELECT execution_profile_id FROM provider_route_members WHERE route_id = $1",
        )
        .bind(light_route_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read light profile: {error}"))?;
        let heavy_profile_id: Uuid = sqlx::query_scalar(
            "SELECT execution_profile_id FROM provider_route_members WHERE route_id = $1",
        )
        .bind(heavy_route_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read heavy profile: {error}"))?;
        let route_id = insert_codex_group_route_with_policy(
            &database.setup_pool,
            &[(light_route_id, 0, 100, 0), (heavy_route_id, 0, 300, 0)],
            "priority_weighted",
            "allow",
        )
        .await?;

        let counts: Vec<(Uuid, i64)> = sqlx::query_as(
            r#"
            WITH jobs AS (
              SELECT md5(series::TEXT)::UUID AS job_id
              FROM generate_series(1, 10000) series
            ), winners AS (
              SELECT jobs.job_id,
                     (
                       SELECT member.execution_profile_id
                       FROM provider_route_members member
                       WHERE member.route_id = $1 AND member.route_revision = 1
                       ORDER BY
                         -LN(
                           (
                             (
                               ('x' || SUBSTR(
                                 md5(
                                   $1::TEXT || ':1:' || jobs.job_id::TEXT || ':'
                                   || member.execution_profile_id::TEXT
                                 ),
                                 1,
                                 15
                               ))::BIT(60)::BIGINT + 1
                             )::NUMERIC / 1152921504606846977::NUMERIC
                           )
                         ) / member.weight::NUMERIC,
                         member.execution_profile_id
                       LIMIT 1
                     ) AS execution_profile_id
              FROM jobs
            )
            SELECT execution_profile_id, COUNT(*)
            FROM winners
            GROUP BY execution_profile_id
            "#,
        )
        .bind(route_id)
        .fetch_all(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to evaluate weighted rendezvous: {error}"))?;
        let light_count = counts
            .iter()
            .find_map(|(profile_id, count)| (*profile_id == light_profile_id).then_some(*count))
            .unwrap_or_default();
        let heavy_count = counts
            .iter()
            .find_map(|(profile_id, count)| (*profile_id == heavy_profile_id).then_some(*count))
            .unwrap_or_default();
        require(
            light_count + heavy_count == 10_000,
            format!("weighted rendezvous lost jobs: {counts:?}"),
        )?;
        require(
            (7_200..=7_800).contains(&heavy_count),
            format!("1:3 weighted share was outside tolerance: {counts:?}"),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn account_control_shrinks_drains_and_rejects_stale_edits() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let account_route_id = insert_codex_route(&database.setup_pool).await?;
        let provider_account_id: Uuid = sqlx::query_scalar(
            "SELECT provider_account_id FROM provider_route_members WHERE route_id = $1",
        )
        .bind(account_route_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read provider account: {error}"))?;
        let service = PostgresProviderManagementService::new(
            database.pool("account_control").await?,
            PathBuf::from("/tmp"),
            PathBuf::from("/bin/false"),
        );
        let shrink = service
            .update_account_scheduling(
                provider_account_id,
                UpdateProviderAccountSchedulingRequest {
                    expected_control_version: 1,
                    max_concurrency: 1,
                    accepting_new_work: true,
                },
            )
            .await
            .map_err(|error| format!("convergent shrink failed: {error:?}"))?;
        require(
            shrink.max_concurrency == 1
                && shrink.allocated_count == 0
                && shrink.control_version == 2,
            "shrink did not advance account control".to_string(),
        )?;
        let stale = service
            .update_account_scheduling(
                provider_account_id,
                UpdateProviderAccountSchedulingRequest {
                    expected_control_version: 1,
                    max_concurrency: 2,
                    accepting_new_work: true,
                },
            )
            .await
            .expect_err("stale account control edit should fail");
        require(
            stale.status_code().as_u16() == 409,
            format!("stale account edit returned {}", stale.status_code()),
        )?;
        let drain = service
            .update_account_scheduling(
                provider_account_id,
                UpdateProviderAccountSchedulingRequest {
                    expected_control_version: 2,
                    max_concurrency: 1,
                    accepting_new_work: false,
                },
            )
            .await
            .map_err(|error| format!("account drain failed: {error:?}"))?;
        require(
            drain.scheduling_state == "draining" && drain.control_version == 3,
            "drain did not advance account lifecycle".to_string(),
        )?;
        let (desired, lifecycle, event_count, hard_max): (i32, String, i64, i32) = sqlx::query_as(
            r#"
                SELECT control.desired_max_concurrency, control.lifecycle_state,
                       COUNT(event.event_id)::BIGINT, policy.max_concurrency
                FROM provider_account_execution_controls control
                JOIN executor_resource_policies policy
                  ON policy.provider_account_id = control.provider_account_id
                 AND policy.state = 'enabled'
                LEFT JOIN provider_account_execution_control_events event
                  ON event.provider_account_id = control.provider_account_id
                WHERE control.provider_account_id = $1
                GROUP BY control.desired_max_concurrency, control.lifecycle_state,
                         policy.max_concurrency
                "#,
        )
        .bind(provider_account_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect account controls: {error}"))?;
        require(
            desired == 1 && lifecycle == "draining" && event_count == 2 && hard_max == 4,
            "account controls or immutable hard ceiling diverged".to_string(),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn route_update_publishes_an_immutable_revision_and_rejects_stale_edits() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let first = insert_codex_route(&database.setup_pool).await?;
        let second = insert_codex_route(&database.setup_pool).await?;
        let route_id = insert_codex_group_route_with_policy(
            &database.setup_pool,
            &[(first, 0, 100, 0), (second, 0, 100, 0)],
            "quota_aware_least_loaded",
            "allow",
        )
        .await?;
        let account_ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT provider_account_id FROM provider_route_members WHERE route_id = $1 AND route_revision = 1 ORDER BY provider_account_id",
        )
        .bind(route_id)
        .fetch_all(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to read route accounts: {error}"))?;
        let service = PostgresProviderManagementService::new(
            database.pool("route_update").await?,
            PathBuf::from("/tmp"),
            PathBuf::from("/bin/false"),
        );
        let request = UpdateProviderRouteRequest {
            expected_revision: 1,
            display_name: "Codex group v2".to_string(),
            selection_strategy: "priority_weighted".to_string(),
            quota_freshness_ms: 600_000,
            unknown_quota_policy: "block".to_string(),
            members: account_ids
                .iter()
                .enumerate()
                .map(|(index, provider_account_id)| CreateProviderRouteMemberRequest {
                    provider_account_id: *provider_account_id,
                    priority: index as i16,
                    weight: if index == 0 { 100 } else { 300 },
                    minimum_remaining_percent: 10,
                })
                .collect(),
            model_mappings: None,
        };
        let published = service
            .update_route(route_id, request.clone())
            .await
            .map_err(|error| format!("route update failed: {error:?}"))?;
        require(
            published.revision == 2
                && published.display_name == "Codex group v2"
                && published.members.len() == 2,
            "route update returned an incomplete revision".to_string(),
        )?;
        let (head_revision, revision_count): (i64, i64) = sqlx::query_as(
            r#"
            SELECT head.current_revision, COUNT(route.revision)::BIGINT
            FROM provider_route_heads head
            JOIN provider_routes route ON route.route_id = head.route_id
            WHERE head.route_id = $1
            GROUP BY head.current_revision
            "#,
        )
        .bind(route_id)
        .fetch_one(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to inspect route revisions: {error}"))?;
        require(
            head_revision == 2 && revision_count == 2,
            "route head or immutable revision history is incomplete".to_string(),
        )?;
        let stale = service
            .update_route(route_id, request)
            .await
            .expect_err("stale route edit should fail");
        require(
            stale.status_code().as_u16() == 409,
            format!("stale route edit returned {}", stale.status_code()),
        )?;
        require(
            sqlx::query(
                "UPDATE provider_route_members SET weight = 1 WHERE route_id = $1 AND route_revision = 2",
            )
            .bind(route_id)
            .execute(&database.setup_pool)
            .await
            .is_err(),
            "published route revision remained mutable".to_string(),
        )?;
        require(
            sqlx::query(
                "UPDATE provider_route_model_mappings SET public_model_id = 'changed' WHERE route_id = $1 AND route_revision = 2",
            )
            .bind(route_id)
            .execute(&database.setup_pool)
            .await
            .is_err(),
            "published route model mappings remained mutable".to_string(),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

async fn revoke_authentication_race(database: &TestDatabase) -> TestResult {
    let project_id = format!("proj_{}", Uuid::new_v4().simple());
    insert_project(&database.setup_pool, &project_id).await?;
    let keyring = test_keyring();
    let setup_store = PostgresApiKeyStore::new(database.pool("setup").await?, keyring.clone());
    let account = setup_store
        .create_service_account(
            &project_id,
            "Race test",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(|error| format!("failed to create API key: {error:?}"))?;
    let key_id = account.api_key.id;
    let key_value = account.api_key.value;
    let service_account_id = account.id;

    let guard_pool = database.pool("guard").await?;
    let mut guard = guard_pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin row-lock guard: {error}"))?;
    sqlx::query("SELECT id FROM gateway_api_keys WHERE id = $1 FOR UPDATE")
        .bind(&key_id)
        .fetch_one(&mut *guard)
        .await
        .map_err(|error| format!("failed to lock API key row: {error}"))?;

    let monitor = database.pool("monitor").await?;
    let revoke_application = database.application_name("revoke");
    let revoke_store = PostgresApiKeyStore::new(
        database
            .pool_with_application_name(&revoke_application)
            .await?,
        keyring.clone(),
    );
    let revoke_project = project_id.clone();
    let revoke_service_account_id = service_account_id.clone();
    let revoke = tokio::spawn(async move {
        revoke_store
            .delete_service_account(&revoke_project, &revoke_service_account_id)
            .await
    });

    if let Err(error) = wait_for_database_lock(&monitor, &revoke_application).await {
        guard.rollback().await.ok();
        abort_and_await(revoke).await;
        return Err(format!("revoke must wait on the API key row lock: {error}"));
    }

    let authenticate_application = database.application_name("authenticate");
    let authenticate_pool = match database
        .pool_with_application_name(&authenticate_application)
        .await
    {
        Ok(pool) => pool,
        Err(error) => {
            guard.rollback().await.ok();
            abort_and_await(revoke).await;
            return Err(error);
        }
    };
    let authenticate_store = PostgresApiKeyStore::new(authenticate_pool, keyring);
    let authenticate =
        tokio::spawn(async move { authenticate_store.authenticate(&key_value).await });

    if let Err(error) = wait_for_database_lock(&monitor, &authenticate_application).await {
        guard.rollback().await.ok();
        abort_and_await(revoke).await;
        abort_and_await(authenticate).await;
        return Err(format!(
            "authenticate mutation must wait behind the queued revoke: {error}"
        ));
    }

    if let Err(error) = guard.commit().await {
        abort_and_await(revoke).await;
        abort_and_await(authenticate).await;
        return Err(format!("failed to release API key row lock: {error}"));
    }

    let revoke_result = join_gateway(revoke, "revoke").await;
    let authenticate_result = join_gateway(authenticate, "authenticate").await;
    revoke_result?;
    let authenticated = authenticate_result?;
    require(
        authenticated.is_none(),
        "authentication must return None after the earlier queued revoke commits".to_string(),
    )
}

async fn sequential_authentication_and_delete(database: &TestDatabase) -> TestResult {
    let project_id = format!("proj_{}", Uuid::new_v4().simple());
    insert_project(&database.setup_pool, &project_id).await?;
    let store = PostgresApiKeyStore::new(database.pool("sequential").await?, test_keyring());
    let account = store
        .create_service_account(
            &project_id,
            "Sequential test",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(|error| format!("failed to create API key: {error:?}"))?;

    require(
        store
            .authenticate(&account.api_key.value)
            .await
            .map_err(|error| format!("initial authentication failed: {error:?}"))?
            .is_some(),
        "new API key should authenticate".to_string(),
    )?;
    let version_before: String =
        sqlx::query_scalar("SELECT xmin::TEXT FROM gateway_api_keys WHERE id = $1")
            .bind(&account.api_key.id)
            .fetch_one(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to read key row version: {error}"))?;
    require(
        store
            .authenticate(&account.api_key.value)
            .await
            .map_err(|error| format!("repeat authentication failed: {error:?}"))?
            .is_some(),
        "repeat API key authentication failed".to_string(),
    )?;
    let version_after: String =
        sqlx::query_scalar("SELECT xmin::TEXT FROM gateway_api_keys WHERE id = $1")
            .bind(&account.api_key.id)
            .fetch_one(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to reread key row version: {error}"))?;
    require(
        version_after == version_before,
        "last_used_at coalescing still created a row version per request".to_string(),
    )?;
    store
        .delete_service_account(&project_id, &account.id)
        .await
        .map_err(|error| format!("failed to delete API key: {error:?}"))?;
    require(
        store
            .authenticate(&account.api_key.value)
            .await
            .map_err(|error| format!("post-delete authentication failed: {error:?}"))?
            .is_none(),
        "deleted API key must stay rejected".to_string(),
    )
}

async fn pepper_rotation_case(database: &TestDatabase) -> TestResult {
    let project_v1 = format!("proj_{}", Uuid::new_v4().simple());
    insert_project(&database.setup_pool, &project_v1).await?;
    let v1_store = PostgresApiKeyStore::new(database.pool("pepper_v1").await?, keyring_v1());
    let v1_account = v1_store
        .create_service_account(
            &project_v1,
            "Pepper v1",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(|error| format!("failed to create v1 key: {error:?}"))?;

    let metadata: (String, Option<i32>, String) = sqlx::query_as(
        "SELECT hash_algorithm, pepper_version, key_hash FROM gateway_api_keys WHERE id = $1",
    )
    .bind(&v1_account.api_key.id)
    .fetch_one(&database.setup_pool)
    .await
    .map_err(|error| format!("failed to inspect v1 key metadata: {error}"))?;
    require(
        metadata.0 == "hmac-sha256-v1" && metadata.1 == Some(1),
        format!("new key did not use versioned HMAC: {metadata:?}"),
    )?;
    require(
        metadata.2 != legacy_digest(&v1_account.api_key.value),
        "new key was stored as an unpeppered SHA-256 digest".to_string(),
    )?;

    let rotated = PostgresApiKeyStore::new(database.pool("pepper_v2").await?, keyring_v2());
    require(
        rotated
            .authenticate(&v1_account.api_key.value)
            .await
            .map_err(|error| format!("rotated keyring failed to verify v1 key: {error:?}"))?
            .is_some(),
        "rotated keyring did not retain v1 verification".to_string(),
    )?;
    let project_v2 = format!("proj_{}", Uuid::new_v4().simple());
    insert_project(&database.setup_pool, &project_v2).await?;
    let v2_account = rotated
        .create_service_account(
            &project_v2,
            "Pepper v2",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(|error| format!("failed to create v2 key: {error:?}"))?;
    let v2_version: Option<i32> =
        sqlx::query_scalar("SELECT pepper_version FROM gateway_api_keys WHERE id = $1")
            .bind(&v2_account.api_key.id)
            .fetch_one(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to inspect v2 key version: {error}"))?;
    require(
        v2_version == Some(2),
        format!("rotated key was not created with v2: {v2_version:?}"),
    )?;

    let retired =
        PostgresApiKeyStore::new(database.pool("pepper_retired").await?, keyring_v2_only());
    require(
        retired
            .authenticate(&v1_account.api_key.value)
            .await
            .map_err(|error| format!("retired keyring v1 check failed: {error:?}"))?
            .is_none(),
        "v1 key remained valid after v1 pepper was removed".to_string(),
    )?;
    require(
        retired
            .authenticate(&v2_account.api_key.value)
            .await
            .map_err(|error| format!("retired keyring v2 check failed: {error:?}"))?
            .is_some(),
        "v2 key stopped authenticating after v1 retirement".to_string(),
    )
}

async fn legacy_key_case(database: &TestDatabase) -> TestResult {
    let project_id = format!("proj_{}", Uuid::new_v4().simple());
    insert_project(&database.setup_pool, &project_id).await?;
    let service_account_id = format!("svc_acct_{}", Uuid::new_v4().simple());
    let key_id = format!("key_{}", Uuid::new_v4().simple());
    let bearer = format!("sk-gw-legacy-{}", Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO gateway_service_accounts (id, project_id, tenant_id, name, role, created_at) VALUES ($1, $2, $2, 'Legacy', 'member', 1)",
    )
    .bind(&service_account_id)
    .bind(&project_id)
    .execute(&database.setup_pool)
    .await
    .map_err(|error| format!("failed to insert legacy service account: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO gateway_api_keys
          (id, project_id, tenant_id, service_account_id, name, key_hash, redacted_value, created_at)
        VALUES ($1, $2, $2, $3, 'Legacy Key', $4, 'sk-gw-...legacy', 1)
        "#,
    )
    .bind(&key_id)
    .bind(&project_id)
    .bind(&service_account_id)
    .bind(legacy_digest(&bearer))
    .execute(&database.setup_pool)
    .await
    .map_err(|error| format!("failed to insert legacy API key: {error}"))?;

    let disabled =
        PostgresApiKeyStore::new(database.pool("legacy_disabled").await?, test_keyring());
    require(
        disabled
            .authenticate(&bearer)
            .await
            .map_err(|error| format!("default legacy rejection failed: {error:?}"))?
            .is_none(),
        "legacy key authenticated without the migration switch".to_string(),
    )?;

    let store = PostgresApiKeyStore::new(
        database.pool("legacy").await?,
        test_keyring().with_legacy_sha256(true),
    );
    let auth = store
        .authenticate(&bearer)
        .await
        .map_err(|error| format!("legacy authentication failed: {error:?}"))?;
    require(
        auth.is_some_and(|context| context.project_id == project_id),
        "legacy key was not accepted during migration window".to_string(),
    )
}

async fn concurrent_authentication_case(database: &TestDatabase) -> TestResult {
    const CLIENTS: usize = 16;
    let project_id = format!("proj_{}", Uuid::new_v4().simple());
    insert_project(&database.setup_pool, &project_id).await?;
    let keyring = test_keyring();
    let setup = PostgresApiKeyStore::new(database.pool("auth_setup").await?, keyring.clone());
    let account = setup
        .create_service_account(
            &project_id,
            "Concurrent auth",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(|error| format!("failed to create concurrent auth key: {error:?}"))?;
    sqlx::query("UPDATE gateway_api_keys SET last_used_at = NULL WHERE id = $1")
        .bind(&account.api_key.id)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to reset last_used_at: {error}"))?;

    let barrier = std::sync::Arc::new(Barrier::new(CLIENTS));
    let mut tasks = Vec::new();
    for index in 0..CLIENTS {
        let store = PostgresApiKeyStore::new(
            database.pool(&format!("auth_{index}")).await?,
            keyring.clone(),
        );
        let bearer = account.api_key.value.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store.authenticate(&bearer).await
        }));
    }

    timeout(WAIT_TIMEOUT, async {
        for task in tasks {
            let authenticated = task
                .await
                .map_err(|error| format!("authentication task failed: {error}"))?
                .map_err(|error| format!("concurrent authentication failed: {error:?}"))?;
            require(
                authenticated.is_some(),
                "concurrent valid key was rejected".to_string(),
            )?;
        }
        Ok(())
    })
    .await
    .map_err(|_| "concurrent authentication deadlocked".to_string())?
}

async fn insert_project(pool: &PgPool, project_id: &str) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO gateway_projects (id, tenant_id, name, created_at)
        VALUES ($1, $1, 'Test project', 1)
        "#,
    )
    .bind(project_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert test project: {error}"))?;
    Ok(())
}

async fn insert_project_member(pool: &PgPool, project_id: &str, user_id: Uuid) -> TestResult {
    let email = format!("member-{}@personal-key.test", user_id.simple());
    sqlx::query(
        r#"
        INSERT INTO identity_users (
          user_id, normalized_email, display_name, roles, scopes,
          created_at_ms, updated_at_ms
        )
        VALUES (
          $1, $2, 'Personal key owner', ARRAY['member'],
          ARRAY['workspace:read', 'workspace:write'], 1, 1
        )
        "#,
    )
    .bind(user_id)
    .bind(email)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert API key owner: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO identity_organizations (
          organization_id, display_name, organization_kind,
          created_at_ms, updated_at_ms
        )
        VALUES ($1, 'API key test workspace', 'system', 1, 1)
        "#,
    )
    .bind(project_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert API key test organization: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO identity_organization_memberships (
          organization_id, user_id, role, state, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'member', 'active', 1, 1)
        "#,
    )
    .bind(project_id)
    .bind(user_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert organization membership: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO identity_project_memberships (
          organization_id, project_id, user_id, role, state, is_default,
          created_at_ms, updated_at_ms
        )
        VALUES ($1, $1, $2, 'member', 'active', FALSE, 1, 1)
        "#,
    )
    .bind(project_id)
    .bind(user_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert project membership: {error}"))?;
    Ok(())
}

async fn wait_for_database_lock(pool: &PgPool, application_name: &str) -> TestResult {
    timeout(WAIT_TIMEOUT, async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM pg_stat_activity
                  WHERE application_name = $1
                    AND state = 'active'
                    AND wait_event_type = 'Lock'
                    AND cardinality(pg_blocking_pids(pid)) > 0
                )
                "#,
            )
            .bind(application_name)
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to inspect pg_stat_activity: {error}"))?;
            if waiting {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| format!("{application_name:?} did not enter lock wait within 5s"))?
}

async fn join_gateway<T>(
    mut task: JoinHandle<Result<T, ImageGatewayError>>,
    operation: &str,
) -> TestResult<T> {
    match timeout(WAIT_TIMEOUT, &mut task).await {
        Ok(result) => result
            .map_err(|error| format!("{operation} task failed: {error}"))?
            .map_err(|error| format!("{operation} should succeed: {error:?}")),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(format!("watchdog timed out waiting for {operation}"))
        }
    }
}

async fn abort_and_await<T>(task: JoinHandle<T>) {
    task.abort();
    let _ = task.await;
}

fn require(condition: bool, message: String) -> TestResult {
    if condition { Ok(()) } else { Err(message) }
}

fn test_keyring() -> ApiKeyKeyring {
    keyring_v1()
}

fn keyring_v1() -> ApiKeyKeyring {
    ApiKeyKeyring::new(1, [(1, vec![0x11; 32])]).expect("v1 test keyring must be valid")
}

fn keyring_v2() -> ApiKeyKeyring {
    ApiKeyKeyring::new(2, [(1, vec![0x11; 32]), (2, vec![0x22; 32])])
        .expect("rotated test keyring must be valid")
}

fn keyring_v2_only() -> ApiKeyKeyring {
    ApiKeyKeyring::new(2, [(2, vec![0x22; 32])]).expect("v2 test keyring must be valid")
}

fn legacy_digest(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

async fn insert_test_video_route(
    pool: &PgPool,
    source_route_id: Uuid,
) -> TestResult<(Uuid, String)> {
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to read database clock: {error}"))?;
    let source_profile_id: Uuid = sqlx::query_scalar(
        "SELECT execution_profile_id FROM provider_route_members WHERE route_id = $1 AND route_revision = 1",
    )
    .bind(source_route_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to read source execution profile: {error}"))?;
    let execution_profile_id = Uuid::new_v4();
    let route_id = Uuid::new_v4();
    let video_model_id = format!("test-video-{}", route_id.simple());
    sqlx::query(
        r#"
        INSERT INTO provider_models
          (provider_id, model_id, execution_model_id, media_kind, display_name,
           adapter_state, lifecycle_state, operation_ids, source_kind,
           first_seen_at_ms, last_seen_at_ms, metadata_json)
        VALUES ('openai-codex', $1, $1, 'video', 'Test video',
                'supported', 'enabled', ARRAY['videos.generations'],
                'adapter_contract', $2, $2, '{}'::JSONB)
        "#,
    )
    .bind(&video_model_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert test video model: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_execution_profiles
          (execution_profile_id, profile_key, provider_id, command_schema,
           operation_id, operation_descriptor_revision,
           operation_descriptor_sha256_v1, completion_mode, idempotency_mode,
           adapter_revision, credential_pool_id, provider_account_id,
           credential_ref, credential_revision, resource_policy_id,
           resource_policy_revision, state, created_at_ms, updated_at_ms)
        SELECT $1, $2, provider_id, 'test.videos.generation.v1',
               'videos.generations', 'test/videos.generations/v1', $3,
               'remote_task', 'provider_token', 'test-video-v1',
               credential_pool_id, provider_account_id, credential_ref,
               credential_revision, resource_policy_id,
               resource_policy_revision, 'enabled', $4, $4
        FROM provider_execution_profiles
        WHERE execution_profile_id = $5
        "#,
    )
    .bind(execution_profile_id)
    .bind(format!("profile.{}", execution_profile_id.simple()))
    .bind("c".repeat(64))
    .bind(now)
    .bind(source_profile_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert test video profile: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_routes
          (route_id, revision, route_key, display_name, provider_id, operation_id,
           command_schema, route_kind, selection_strategy, state, created_at_ms)
        VALUES ($1, 1, $2, 'Video test', 'openai-codex',
                'videos.generations', 'test.videos.generation.v1', 'account',
                'quota_aware_least_loaded', 'enabled', $3)
        "#,
    )
    .bind(route_id)
    .bind(format!("route.{}", route_id.simple()))
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert test video route: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_heads
          (route_id, route_key, provider_id, operation_id, command_schema,
           route_kind, current_revision, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'openai-codex', 'videos.generations',
                'test.videos.generation.v1', 'account', 1, 'enabled', $3, $3)
        "#,
    )
    .bind(route_id)
    .bind(format!("route.{}", route_id.simple()))
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert test video route head: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_members
          (route_id, route_revision, provider_id, operation_id, command_schema,
           provider_account_id, execution_profile_id, priority, weight, state,
           minimum_remaining_percent, created_at_ms)
        SELECT $1, 1, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, 7, 37, 'enabled', 12, $2
        FROM provider_execution_profiles WHERE execution_profile_id = $3
        "#,
    )
    .bind(route_id)
    .bind(now)
    .bind(execution_profile_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert test video route member: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings
          (route_id, route_revision, provider_id, operation_id, command_schema,
           api_profile, public_model_id, provider_model_id, execution_model_id,
           media_kind, created_at_ms)
        VALUES ($1, 1, 'openai-codex', 'videos.generations',
                'test.videos.generation.v1', 'test-videos-v1', $2, $2, $2,
                'video', $3)
        "#,
    )
    .bind(route_id)
    .bind(&video_model_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert test video mapping: {error}"))?;
    Ok((route_id, video_model_id))
}

async fn bind_test_api_key_route(
    pool: &PgPool,
    project_id: &str,
    api_key_id: &str,
    route_id: Uuid,
) -> TestResult {
    let inserted = sqlx::query(
        r#"
        INSERT INTO gateway_api_key_provider_routes
          (api_key_id, service_account_id, project_id, tenant_id, provider_id,
           operation_id, command_schema, route_id, route_revision, bound_at_ms)
        SELECT api_key.id, api_key.service_account_id, api_key.project_id,
               api_key.tenant_id, head.provider_id, head.operation_id,
               head.command_schema, head.route_id, head.current_revision,
               floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
        FROM gateway_api_keys api_key
        CROSS JOIN provider_route_heads head
        WHERE api_key.id = $1 AND api_key.project_id = $2 AND head.route_id = $3
        "#,
    )
    .bind(api_key_id)
    .bind(project_id)
    .bind(route_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to bind test API key route: {error}"))?;
    require(
        inserted.rows_affected() == 1,
        "test API key route binding was not inserted".to_string(),
    )
}

async fn bind_test_console_routes(
    pool: &PgPool,
    project_id: &str,
    platform_route_id: Uuid,
    project_route_id: Uuid,
) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO gateway_platform_provider_routes
          (provider_id, operation_id, command_schema, route_id, route_revision,
           state, created_at_ms, updated_at_ms)
        SELECT provider_id, operation_id, command_schema, route_id,
               current_revision, 'enabled', updated_at_ms, updated_at_ms
        FROM provider_route_heads WHERE route_id = $1
        "#,
    )
    .bind(platform_route_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to bind platform test route: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO gateway_project_provider_routes
          (project_id, provider_id, operation_id, command_schema, route_id,
           route_revision, state, created_at_ms, updated_at_ms)
        SELECT $1, provider_id, operation_id, command_schema, route_id,
               current_revision, 'enabled', updated_at_ms, updated_at_ms
        FROM provider_route_heads WHERE route_id = $2
        "#,
    )
    .bind(project_id)
    .bind(project_route_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to bind project test route: {error}"))?;
    Ok(())
}

async fn replace_current_route_profiles(
    pool: &PgPool,
    route_ids: &[Uuid],
) -> TestResult<Vec<(Uuid, Uuid)>> {
    let old_profile_ids = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT DISTINCT member.execution_profile_id
        FROM provider_route_heads head
        JOIN provider_route_members member
          ON member.route_id = head.route_id
         AND member.route_revision = head.current_revision
         AND member.state = 'enabled'
        WHERE head.route_id = ANY($1)
        ORDER BY member.execution_profile_id
        "#,
    )
    .bind(route_ids)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("failed to read legacy route profiles: {error}"))?;
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin profile replacement: {error}"))?;
    let mut profile_replacements = Vec::new();
    for old_profile_id in old_profile_ids {
        let new_profile_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO provider_execution_profiles
              (execution_profile_id, profile_key, provider_id, command_schema,
               operation_id, operation_descriptor_revision,
               operation_descriptor_sha256_v1, completion_mode, idempotency_mode,
               adapter_revision, credential_pool_id, provider_account_id,
               credential_ref, credential_revision, resource_policy_id,
               resource_policy_revision, state, created_at_ms, updated_at_ms)
            SELECT $1, $2, provider_id, command_schema, operation_id,
                   operation_descriptor_revision, operation_descriptor_sha256_v1,
                   completion_mode, idempotency_mode,
                   adapter_revision || '.runtime-v1', credential_pool_id,
                   provider_account_id, credential_ref, credential_revision,
                   resource_policy_id, resource_policy_revision, 'enabled',
                   updated_at_ms + 1, updated_at_ms + 1
            FROM provider_execution_profiles
            WHERE execution_profile_id = $3
            "#,
        )
        .bind(new_profile_id)
        .bind(format!("runtime.{}", new_profile_id.simple()))
        .bind(old_profile_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("failed to insert replacement profile: {error}"))?;
        sqlx::query(
            "UPDATE provider_execution_profiles SET state = 'disabled', updated_at_ms = updated_at_ms + 1 WHERE execution_profile_id = $1",
        )
        .bind(old_profile_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("failed to disable legacy profile: {error}"))?;
        profile_replacements.push((old_profile_id, new_profile_id));
    }
    tx.commit()
        .await
        .map_err(|error| format!("failed to commit profile replacement: {error}"))?;

    let mut route_replacements = Vec::new();
    for route_id in route_ids {
        let old_profile_id: Uuid = sqlx::query_scalar(
            "SELECT execution_profile_id FROM provider_route_members WHERE route_id = $1 AND route_revision = 1 AND state = 'enabled' LIMIT 1",
        )
        .bind(route_id)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to read route legacy profile: {error}"))?;
        let new_profile_id = profile_replacements
            .iter()
            .find_map(|(old, new)| (*old == old_profile_id).then_some(*new))
            .ok_or_else(|| "route replacement profile was not created".to_string())?;
        route_replacements.push((*route_id, new_profile_id));
    }
    Ok(route_replacements)
}

async fn insert_legacy_codex_snapshot_mappings(pool: &PgPool, route_ids: &[Uuid]) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO provider_models
          (provider_id, model_id, execution_model_id, media_kind, display_name,
           adapter_state, lifecycle_state, operation_ids, source_kind,
           first_seen_at_ms, last_seen_at_ms, metadata_json)
        VALUES ('openai-codex', 'gpt-image-2-2026-04-21',
                'gpt-image-2-2026-04-21', 'image', 'GPT Image 2 snapshot',
                'supported', 'enabled',
                ARRAY['images.generations', 'images.edits'],
                'adapter_contract',
                floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
                floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
                '{}'::JSONB)
        ON CONFLICT (provider_id, model_id, media_kind) DO NOTHING
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert legacy snapshot model: {error}"))?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings
          (route_id, route_revision, provider_id, operation_id, command_schema,
           api_profile, public_model_id, provider_model_id, execution_model_id,
           media_kind, created_at_ms)
        SELECT head.route_id, head.current_revision, head.provider_id,
               head.operation_id, head.command_schema, 'openai-images-v1',
               'gpt-image-2-2026-04-21', 'gpt-image-2-2026-04-21',
               'gpt-image-2-2026-04-21', 'image', head.updated_at_ms
        FROM provider_route_heads head
        WHERE head.route_id = ANY($1)
        "#,
    )
    .bind(route_ids)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert legacy snapshot mappings: {error}"))?;
    require(
        inserted.rows_affected() == route_ids.len() as u64,
        format!(
            "inserted {} legacy snapshot mappings",
            inserted.rows_affected()
        ),
    )
}

async fn assert_codex_snapshot_mapping_reconciled(pool: &PgPool, route_ids: &[Uuid]) -> TestResult {
    let state: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT COUNT(*)
           FROM provider_route_model_mappings mapping
           WHERE mapping.route_id = ANY($1)
             AND mapping.route_revision = 1
             AND mapping.public_model_id = 'gpt-image-2-2026-04-21'
             AND mapping.provider_model_id = 'gpt-image-2-2026-04-21'
             AND mapping.execution_model_id = 'gpt-image-2-2026-04-21'),
          (SELECT COUNT(*)
           FROM provider_route_heads head
           JOIN provider_route_model_mappings mapping
             ON mapping.route_id = head.route_id
            AND mapping.route_revision = head.current_revision
           WHERE head.route_id = ANY($1)
             AND mapping.public_model_id = 'gpt-image-2-2026-04-21'
             AND mapping.provider_model_id = 'gpt-image-2'
             AND mapping.execution_model_id = 'gpt-image-2-2026-04-21'),
          (SELECT COUNT(*) FROM provider_routes route
           WHERE route.route_id = ANY($1))
        "#,
    )
    .bind(route_ids)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect snapshot route reconciliation: {error}"))?;
    require(
        state
            == (
                route_ids.len() as i64,
                route_ids.len() as i64,
                (route_ids.len() * 2) as i64,
            ),
        format!("snapshot route reconciliation lost immutable identity: {state:?}"),
    )
}

async fn assert_reconciled_route_copies(pool: &PgPool, route_ids: &[Uuid]) -> TestResult {
    let heads_at_revision_two: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM provider_route_heads WHERE route_id = ANY($1) AND current_revision = 2",
    )
    .bind(route_ids)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect reconciled route heads: {error}"))?;
    let lost_mappings: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM provider_route_model_mappings old_mapping
        LEFT JOIN provider_route_model_mappings new_mapping
          ON new_mapping.route_id = old_mapping.route_id
         AND new_mapping.route_revision = 2
         AND new_mapping.provider_id = old_mapping.provider_id
         AND new_mapping.operation_id = old_mapping.operation_id
         AND new_mapping.command_schema = old_mapping.command_schema
         AND new_mapping.api_profile = old_mapping.api_profile
         AND new_mapping.public_model_id = old_mapping.public_model_id
         AND new_mapping.provider_model_id = CASE
           WHEN old_mapping.provider_id = 'openai-codex'
            AND old_mapping.provider_model_id = 'gpt-image-2-2026-04-21'
            AND old_mapping.execution_model_id = 'gpt-image-2-2026-04-21'
           THEN 'gpt-image-2'
           ELSE old_mapping.provider_model_id
         END
         AND new_mapping.execution_model_id = old_mapping.execution_model_id
         AND new_mapping.media_kind = old_mapping.media_kind
        WHERE old_mapping.route_id = ANY($1)
          AND old_mapping.route_revision = 1
          AND new_mapping.route_id IS NULL
        "#,
    )
    .bind(route_ids)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to compare route mappings: {error}"))?;
    let changed_member_policies: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM provider_route_members old_member
        JOIN provider_route_members new_member
          ON new_member.route_id = old_member.route_id
         AND new_member.route_revision = 2
         AND new_member.provider_account_id = old_member.provider_account_id
        WHERE old_member.route_id = ANY($1)
          AND old_member.route_revision = 1
          AND (
            new_member.priority <> old_member.priority
            OR new_member.weight <> old_member.weight
            OR new_member.minimum_remaining_percent <>
               old_member.minimum_remaining_percent
            OR new_member.state <> old_member.state
          )
        "#,
    )
    .bind(route_ids)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to compare route member policies: {error}"))?;
    let inactive_current_profiles: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM provider_route_heads head
        JOIN provider_route_members member
          ON member.route_id = head.route_id
         AND member.route_revision = head.current_revision
         AND member.state = 'enabled'
        JOIN provider_execution_profiles profile
          ON profile.execution_profile_id = member.execution_profile_id
        WHERE head.route_id = ANY($1) AND profile.state <> 'enabled'
        "#,
    )
    .bind(route_ids)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect current route profiles: {error}"))?;
    require(
        heads_at_revision_two == route_ids.len() as i64
            && lost_mappings == 0
            && changed_member_policies == 0
            && inactive_current_profiles == 0,
        format!(
            "route copy invariants failed: heads={heads_at_revision_two}, lost_mappings={lost_mappings}, changed_policies={changed_member_policies}, inactive_profiles={inactive_current_profiles}"
        ),
    )
}

async fn insert_codex_route(pool: &PgPool) -> TestResult<Uuid> {
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to read database clock: {error}"))?;
    let credential_pool_id = Uuid::new_v4();
    let provider_account_id = Uuid::new_v4();
    let resource_policy_id = Uuid::new_v4();
    let execution_profile_id = Uuid::new_v4();
    let route_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_models
          (provider_id, model_id, execution_model_id, media_kind, display_name,
           adapter_state, lifecycle_state, operation_ids, source_kind,
           first_seen_at_ms, last_seen_at_ms, metadata_json)
        VALUES ('openai-codex', 'gpt-image-2', 'gpt-image-2', 'image',
                'GPT Image 2', 'supported', 'enabled',
                ARRAY['images.generations', 'images.edits'], 'adapter_contract',
                $1, $1, '{}'::JSONB)
        ON CONFLICT (provider_id, model_id, media_kind) DO NOTHING
        "#,
    )
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Codex model: {error}"))?;
    sqlx::query(
        "INSERT INTO provider_credential_pools (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 'openai-codex', 'enabled', $3, $3)",
    )
    .bind(credential_pool_id)
    .bind(format!("pool.{}", credential_pool_id.simple()))
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert credential pool: {error}"))?;
    sqlx::query(
        "INSERT INTO provider_accounts (provider_account_id, credential_pool_id, provider_id, account_key, credential_ref, credential_revision, credential_auth_sha256, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 'openai-codex', $3, $4, 1, $5, 'enabled', $6, $6)",
    )
    .bind(provider_account_id)
    .bind(credential_pool_id)
    .bind(format!("account.{}", provider_account_id.simple()))
    .bind(format!("test.codex.{}.1", provider_account_id.simple()))
    .bind("a".repeat(64))
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert provider account: {error}"))?;
    sqlx::query(
        "INSERT INTO executor_resource_policies (resource_policy_id, revision, credential_pool_id, provider_account_id, provider_id, execution_class, max_concurrency, state, created_at_ms) VALUES ($1, 1, $2, $3, 'openai-codex', 'agentic-cli', 4, 'enabled', $4)",
    )
    .bind(resource_policy_id)
    .bind(credential_pool_id)
    .bind(provider_account_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert resource policy: {error}"))?;
    sqlx::query(
        "INSERT INTO provider_execution_profiles (execution_profile_id, profile_key, provider_id, command_schema, operation_id, operation_descriptor_revision, operation_descriptor_sha256_v1, completion_mode, idempotency_mode, adapter_revision, credential_pool_id, provider_account_id, credential_ref, credential_revision, resource_policy_id, resource_policy_revision, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 'openai-codex', 'openai.images.generation.v1', 'images.generations', 'openai-codex/images.generations/v1', $3, 'inline', 'submission_bound', 'openai-codex-generation-v1', $4, $5, $6, 1, $7, 1, 'enabled', $8, $8)",
    )
    .bind(execution_profile_id)
    .bind(format!("profile.{}", execution_profile_id.simple()))
    .bind("b".repeat(64))
    .bind(credential_pool_id)
    .bind(provider_account_id)
    .bind(format!("test.codex.{}.1", provider_account_id.simple()))
    .bind(resource_policy_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert execution profile: {error}"))?;
    sqlx::query(
        "INSERT INTO provider_account_environments (provider_account_id, provider_id, environment_kind, environment_ref, upstream_identity_sha256, display_name, state, created_at_ms, updated_at_ms) VALUES ($1, 'openai-codex', 'codex_home_v1', $2, $3, 'Codex test', 'active', $4, $4)",
    )
    .bind(provider_account_id)
    .bind(format!("/tmp/codex-test-{}", provider_account_id.simple()))
    .bind(hex::encode(Sha256::digest(provider_account_id.as_bytes())))
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert account environment: {error}"))?;
    sqlx::query(
        "INSERT INTO provider_routes (route_id, revision, route_key, display_name, provider_id, operation_id, command_schema, route_kind, selection_strategy, state, created_at_ms) VALUES ($1, 1, $2, 'Codex test', 'openai-codex', 'images.generations', 'openai.images.generation.v1', 'account', 'quota_aware_least_loaded', 'enabled', $3)",
    )
    .bind(route_id)
    .bind(format!("route.{}", route_id.simple()))
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert provider route: {error}"))?;
    sqlx::query(
        "INSERT INTO provider_route_heads (route_id, route_key, provider_id, operation_id, command_schema, route_kind, current_revision, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 'openai-codex', 'images.generations', 'openai.images.generation.v1', 'account', 1, 'enabled', $3, $3)",
    )
    .bind(route_id)
    .bind(format!("route.{}", route_id.simple()))
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert provider route head: {error}"))?;
    sqlx::query(
        "INSERT INTO provider_route_members (route_id, route_revision, provider_id, operation_id, command_schema, provider_account_id, execution_profile_id, created_at_ms) VALUES ($1, 1, 'openai-codex', 'images.generations', 'openai.images.generation.v1', $2, $3, $4)",
    )
    .bind(route_id)
    .bind(provider_account_id)
    .bind(execution_profile_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert provider route member: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings
          (route_id, route_revision, provider_id, operation_id, command_schema,
           api_profile, public_model_id, provider_model_id, execution_model_id,
           media_kind, created_at_ms)
        VALUES ($1, 1, 'openai-codex', 'images.generations',
                'openai.images.generation.v1', 'openai-images-v1',
                'gpt-image-2', 'gpt-image-2', 'gpt-image-2', 'image', $2)
        "#,
    )
    .bind(route_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert route model mapping: {error}"))?;
    Ok(route_id)
}

async fn insert_codex_group_route_with_policy(
    pool: &PgPool,
    members: &[(Uuid, i16, i32, i16)],
    selection_strategy: &str,
    unknown_quota_policy: &str,
) -> TestResult<Uuid> {
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to read database clock: {error}"))?;
    let route_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO provider_routes (route_id, revision, route_key, display_name, provider_id, operation_id, command_schema, route_kind, selection_strategy, unknown_quota_policy, state, created_at_ms) VALUES ($1, 1, $2, 'Codex quota group', 'openai-codex', 'images.generations', 'openai.images.generation.v1', 'group', $3, $4, 'enabled', $5)",
    )
    .bind(route_id)
    .bind(format!("group.{}", route_id.simple()))
    .bind(selection_strategy)
    .bind(unknown_quota_policy)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert provider group route: {error}"))?;
    sqlx::query(
        "INSERT INTO provider_route_heads (route_id, route_key, provider_id, operation_id, command_schema, route_kind, current_revision, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 'openai-codex', 'images.generations', 'openai.images.generation.v1', 'group', 1, 'enabled', $3, $3)",
    )
    .bind(route_id)
    .bind(format!("group.{}", route_id.simple()))
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert provider group route head: {error}"))?;
    let mut inserted = 0_u64;
    for (account_route_id, priority, weight, minimum_remaining_percent) in members {
        inserted += sqlx::query(
            r#"
            INSERT INTO provider_route_members
              (route_id, route_revision, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, priority, weight, state,
               minimum_remaining_percent, created_at_ms)
            SELECT $1, 1, provider_id, operation_id, command_schema,
                   provider_account_id, execution_profile_id, $2, $3,
                   state, $4, $5
            FROM provider_route_members
            WHERE route_id = $6 AND state = 'enabled'
            "#,
        )
        .bind(route_id)
        .bind(priority)
        .bind(weight)
        .bind(minimum_remaining_percent)
        .bind(now)
        .bind(account_route_id)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to insert provider group member: {error}"))?
        .rows_affected();
    }
    require(
        inserted == members.len() as u64,
        format!("group route inserted {inserted} members"),
    )?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings
          (route_id, route_revision, provider_id, operation_id, command_schema,
           api_profile, public_model_id, provider_model_id, execution_model_id,
           media_kind, created_at_ms)
        VALUES ($1, 1, 'openai-codex', 'images.generations',
                'openai.images.generation.v1', 'openai-images-v1',
                'gpt-image-2', 'gpt-image-2', 'gpt-image-2', 'image', $2)
        "#,
    )
    .bind(route_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert group route model mapping: {error}"))?;
    Ok(route_id)
}

struct TestDatabase {
    database_url: String,
    schema: String,
    setup_pool: PgPool,
    application_prefix: String,
}

impl TestDatabase {
    async fn new() -> TestResult<Option<Self>> {
        let Ok(database_url) = env::var(DATABASE_URL) else {
            if env::var_os("CI").is_some() {
                return Err(format!("{DATABASE_URL} must be set when CI is present"));
            }
            eprintln!("skipping PostgreSQL API key test: {DATABASE_URL} is not set");
            return Ok(None);
        };
        let suffix = Uuid::new_v4().simple().to_string();
        let schema = format!("image_gateway_api_key_test_{suffix}");
        let application_prefix = format!("api_key_test_{}", &suffix[..12]);
        let setup_pool = connect_pool(&database_url, &schema, &application_prefix).await?;

        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&setup_pool)
            .await
            .map_err(|error| format!("failed to identify test database: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            setup_pool.close().await;
            return Err(format!(
                "refusing schema DDL because current_database() is {database_name:?}, which does not contain 'test'"
            ));
        }

        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&setup_pool)
            .await
            .map_err(|error| format!("failed to create isolated schema {schema}: {error}"))?;
        if let Err(error) = run_migrations(&setup_pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&setup_pool)
                .await;
            setup_pool.close().await;
            return Err(format!("failed to migrate isolated schema: {error:?}"));
        }

        Ok(Some(Self {
            database_url,
            schema,
            setup_pool,
            application_prefix,
        }))
    }

    fn application_name(&self, role: &str) -> String {
        format!("{}_{}", self.application_prefix, role)
    }

    async fn pool(&self, role: &str) -> TestResult<PgPool> {
        self.pool_with_application_name(&self.application_name(role))
            .await
    }

    async fn pool_with_application_name(&self, application_name: &str) -> TestResult<PgPool> {
        connect_pool(&self.database_url, &self.schema, application_name).await
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.setup_pool)
        .await
        .map_err(|error| format!("failed to clean isolated schema {}: {error}", self.schema));
        self.setup_pool.close().await;
        result.map(|_| ())
    }
}

async fn connect_pool(
    database_url: &str,
    schema: &str,
    application_name: &str,
) -> TestResult<PgPool> {
    let options = PgConnectOptions::from_str(database_url)
        .map_err(|error| format!("invalid test database URL: {error}"))?
        .application_name(application_name)
        .options([("search_path", schema)]);
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| format!("test database should be reachable: {error}"))
}
