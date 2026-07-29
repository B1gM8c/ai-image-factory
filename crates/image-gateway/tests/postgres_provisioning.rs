use std::env;

use gpt_image_2_gateway::provider_management::{
    PostgresProviderManagementService, ProviderAccountModelSelection, ProviderManagementService,
    ProviderRouteModelMappingRequest, UpdateProviderAccountModelConfigurationRequest,
};
use gpt_image_2_gateway::{
    CodexExecutionProfileProvisioning, CodexProfileProvisioningError,
    DreaminaExecutionProfileProvisioning, ExecutorExecutionProfileStore, ExecutorProfileBinding,
    GrokExecutionProfileProvisioning, PostgresExecutorSubmissionStore,
    database::{connect_test_pool_with_search_path, run_migrations},
    identify_executor_profile_binding, provision_codex_execution_profile,
    provision_dreamina_execution_profile, provision_dreamina_video_execution_profile,
    provision_grok_execution_profile, provision_grok_video_execution_profile,
};
use image_provider_dreamina_cli::{
    DREAMINA_IMAGE_GENERATION_OPERATION_V1, DREAMINA_SUBMIT_COMMAND_SCHEMA,
    DREAMINA_VIDEO_GENERATION_OPERATION_V1, PROVIDER_ID as DREAMINA_PROVIDER_ID,
};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn dreamina_profile_provisions_with_an_isolated_keyring_credential() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = dreamina_fixture("binding");
        let provisioned = provision_dreamina_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        let loaded = PostgresExecutorSubmissionStore::new(database.pool.clone())
            .load_execution_profile(&provisioning.profile_key)
            .await
            .map_err(debug_error)?;
        let operation = DREAMINA_IMAGE_GENERATION_OPERATION_V1;
        require(
            loaded.execution_profile_id == provisioned.execution_profile_id
                && loaded.provider_id == DREAMINA_PROVIDER_ID
                && loaded.command_schema == DREAMINA_SUBMIT_COMMAND_SCHEMA
                && loaded.operation_id == operation.id
                && loaded.operation_descriptor_revision == operation.descriptor_revision
                && loaded.operation_descriptor_sha256_v1 == operation.canonical_sha256_v1_hex(),
            format!("provisioned Dreamina profile has the wrong runtime binding: {loaded:?}"),
        )?;
        let credential_state: (String, String, String) = sqlx::query_as(
            r#"
            SELECT revision.material_kind, head.lifecycle_state, head.refresh_strategy
            FROM provider_account_credential_heads head
            JOIN provider_account_credential_revisions revision
              ON revision.provider_account_id = head.provider_account_id
             AND revision.revision = head.active_revision
            WHERE head.provider_account_id = $1
            "#,
        )
        .bind(provisioned.provider_account_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            credential_state
                == (
                    "system_keyring".to_string(),
                    "active".to_string(),
                    "cli_managed".to_string(),
                ),
            format!("Dreamina credential state is not isolated and runnable: {credential_state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn dreamina_video_profile_has_a_distinct_remote_task_descriptor() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = dreamina_video_fixture("binding");
        let provisioned = provision_dreamina_video_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        let loaded = PostgresExecutorSubmissionStore::new(database.pool.clone())
            .load_execution_profile(&provisioning.profile_key)
            .await
            .map_err(debug_error)?;
        let operation = DREAMINA_VIDEO_GENERATION_OPERATION_V1;
        require(
            loaded.execution_profile_id == provisioned.execution_profile_id
                && loaded.provider_id == DREAMINA_PROVIDER_ID
                && loaded.command_schema == DREAMINA_SUBMIT_COMMAND_SCHEMA
                && loaded.operation_id == operation.id
                && loaded.operation_descriptor_revision == operation.descriptor_revision
                && loaded.operation_descriptor_sha256_v1 == operation.canonical_sha256_v1_hex(),
            format!("provisioned Dreamina video profile has the wrong binding: {loaded:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn existing_dreamina_accounts_gain_one_idempotent_video_profile_and_route() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = dreamina_fixture("backfill");
        let image = provision_dreamina_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_environments
              (provider_account_id, provider_id, environment_kind, environment_ref,
               upstream_identity_sha256, display_name, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'dreamina_home_v1', '/tmp/dreamina-backfill-test',
                    $3, 'Backfill Test', 'active', 1, 1)
            "#,
        )
        .bind(image.provider_account_id)
        .bind(DREAMINA_PROVIDER_ID)
        .bind("e".repeat(64))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_models
              (provider_id, model_id, execution_model_id, media_kind, display_name,
               adapter_state, lifecycle_state, operation_ids, source_kind,
               first_seen_at_ms, last_seen_at_ms, last_successful_refresh_at_ms)
            VALUES ($1, 'seedance2.0', 'seedance2.0', 'video', 'Seedance 2.0',
                    'supported', 'enabled', ARRAY['videos.generations'],
                    'adapter_contract', 1, 1, 1)
            "#,
        )
        .bind(DREAMINA_PROVIDER_ID)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            PostgresProviderManagementService::reconcile_dreamina_video_profiles(&database.pool)
                .await
                .map_err(debug_error)?
                == 0,
            "Dreamina reconciliation enabled video for an image-only account",
        )?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_operations
              (provider_account_id, provider_id, operation_id, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'videos.generations', 'enabled', 1, 1)
            "#,
        )
        .bind(image.provider_account_id)
        .bind(DREAMINA_PROVIDER_ID)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            PostgresProviderManagementService::reconcile_dreamina_video_profiles(&database.pool)
                .await
                .map_err(debug_error)?
                == 1,
            "first Dreamina reconciliation did not create exactly one video profile",
        )?;
        require(
            PostgresProviderManagementService::reconcile_dreamina_video_profiles(&database.pool)
                .await
                .map_err(debug_error)?
                == 0,
            "second Dreamina reconciliation was not idempotent",
        )?;
        let counts: (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_execution_profiles
               WHERE provider_account_id = $1 AND operation_id = 'videos.generations'),
              (SELECT COUNT(*)
               FROM provider_route_heads head
               JOIN provider_route_members member
                 ON member.route_id = head.route_id
                AND member.route_revision = head.current_revision
               WHERE member.provider_account_id = $1
                 AND head.operation_id = 'videos.generations')
            "#,
        )
        .bind(image.provider_account_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1),
            format!("Dreamina video reconciliation duplicated durable state: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn account_model_permissions_and_route_mappings_commit_atomically() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = fixture("atomic-model-config");
        let provisioned = provision_codex_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        let command_schema: String = sqlx::query_scalar(
            "SELECT command_schema FROM provider_execution_profiles WHERE execution_profile_id = $1",
        )
        .bind(provisioned.execution_profile_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_environments
              (provider_account_id, provider_id, environment_kind, environment_ref,
               upstream_identity_sha256, display_name, state, created_at_ms, updated_at_ms)
            VALUES ($1, 'openai-codex', 'codex_home_v1', '/tmp/codex-atomic-model-config',
                    $2, 'Atomic Codex', 'active', 1, 1)
            "#,
        )
        .bind(provisioned.provider_account_id)
        .bind("f".repeat(64))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_models
              (provider_id, model_id, execution_model_id, media_kind, display_name,
               adapter_state, lifecycle_state, operation_ids, source_kind,
               first_seen_at_ms, last_seen_at_ms, last_successful_refresh_at_ms)
            VALUES ('openai-codex', 'gpt-image-2', 'gpt-image-2', 'image', 'GPT Image 2',
                    'supported', 'enabled', ARRAY['images.generations'],
                    'adapter_contract', 1, 1, 1)
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let route_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO provider_routes
              (route_id, revision, route_key, display_name, provider_id, operation_id,
               command_schema, route_kind, selection_strategy, quota_freshness_ms,
               unknown_quota_policy, state, created_at_ms)
            VALUES ($1, 1, 'account.atomic-model-config', 'Atomic Codex', 'openai-codex',
                    'images.generations', $2, 'account', 'quota_aware_least_loaded',
                    300000, 'block', 'enabled', 1)
            "#,
        )
        .bind(route_id)
        .bind(&command_schema)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_route_heads
              (route_id, route_key, provider_id, operation_id, command_schema,
               route_kind, current_revision, state, created_at_ms, updated_at_ms)
            VALUES ($1, 'account.atomic-model-config', 'openai-codex',
                    'images.generations', $2, 'account', 1, 'enabled', 1, 1)
            "#,
        )
        .bind(route_id)
        .bind(&command_schema)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_route_members
              (route_id, route_revision, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, priority, weight, state,
               minimum_remaining_percent, created_at_ms)
            VALUES ($1, 1, 'openai-codex', 'images.generations', $2,
                    $3, $4, 0, 100, 'enabled', 0, 1)
            "#,
        )
        .bind(route_id)
        .bind(&command_schema)
        .bind(provisioned.provider_account_id)
        .bind(provisioned.execution_profile_id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        let service = PostgresProviderManagementService::new(
            database.pool.clone(),
            std::path::PathBuf::from("/tmp"),
            std::path::PathBuf::from("/usr/bin/true"),
        );
        let request = |public_model_id: &str| UpdateProviderAccountModelConfigurationRequest {
            expected_model_version: 0,
            mode: "allowlist".to_owned(),
            enabled_models: vec![ProviderAccountModelSelection {
                model_id: "gpt-image-2".to_owned(),
                media_kind: "image".to_owned(),
            }],
            route_id,
            expected_route_revision: 1,
            model_mappings: vec![ProviderRouteModelMappingRequest {
                api_profile: "openai-images-v1".to_owned(),
                public_model_id: public_model_id.to_owned(),
                provider_model_id: "gpt-image-2".to_owned(),
                media_kind: "image".to_owned(),
            }],
        };
        service
            .update_provider_account_model_configuration(
                provisioned.provider_account_id,
                request("invalid alias!"),
            )
            .await
            .expect_err("invalid route mapping must roll back the whole command");
        let rolled_back: (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_account_model_configurations
               WHERE provider_account_id = $1),
              (SELECT current_revision FROM provider_route_heads WHERE route_id = $2)
            "#,
        )
        .bind(provisioned.provider_account_id)
        .bind(route_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            rolled_back == (0, 1),
            format!("failed atomic model command left partial state: {rolled_back:?}"),
        )?;

        let updated = service
            .update_provider_account_model_configuration(
                provisioned.provider_account_id,
                request("gpt-image-2"),
            )
            .await
            .map_err(debug_error)?;
        let committed: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_account_model_bindings
               WHERE provider_account_id = $1 AND model_id = 'gpt-image-2'),
              (SELECT current_revision FROM provider_route_heads WHERE route_id = $2),
              (SELECT COUNT(*) FROM provider_route_model_mappings
               WHERE route_id = $2 AND route_revision = 2
                 AND public_model_id = 'gpt-image-2')
            "#,
        )
        .bind(provisioned.provider_account_id)
        .bind(route_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            updated.model_version == 1
                && updated.route_revision == 2
                && committed == (1, 2, 1),
            format!("atomic model command did not commit both sides: {committed:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn grok_profile_provisions_through_the_shared_identity_kernel() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = grok_fixture("binding");
        let provisioned = provision_grok_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        let loaded = PostgresExecutorSubmissionStore::new(database.pool.clone())
            .load_execution_profile(&provisioning.profile_key)
            .await
            .map_err(debug_error)?;
        require(
            loaded.execution_profile_id == provisioned.execution_profile_id
                && identify_executor_profile_binding(&loaded)
                    == Ok(ExecutorProfileBinding::GrokImageGeneration),
            format!("provisioned Grok profile does not match its runtime binding: {loaded:?}"),
        )?;
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles)
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1, 1, 1),
            format!("Grok provisioning duplicated the identity graph: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn grok_video_profile_has_a_distinct_runtime_binding() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = grok_video_fixture("binding");
        let provisioned = provision_grok_video_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        let loaded = PostgresExecutorSubmissionStore::new(database.pool.clone())
            .load_execution_profile(&provisioning.profile_key)
            .await
            .map_err(debug_error)?;
        require(
            loaded.execution_profile_id == provisioned.execution_profile_id
                && identify_executor_profile_binding(&loaded)
                    == Ok(ExecutorProfileBinding::GrokVideoGeneration),
            format!("provisioned Grok video profile has the wrong binding: {loaded:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn exact_enabled_provisioning_is_repeatable() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = fixture("repeatable");
        let first = provision_codex_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        let replay = provision_codex_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        require(
            first == replay,
            "exact provisioning changed durable identities",
        )?;
        let loaded = PostgresExecutorSubmissionStore::new(database.pool.clone())
            .load_execution_profile(&provisioning.profile_key)
            .await
            .map_err(debug_error)?;
        require(
            loaded.execution_profile_id == first.execution_profile_id
                && loaded.credential_pool_id == first.credential_pool_id
                && loaded.provider_account_id == first.provider_account_id
                && loaded.resource_policy_id == first.resource_policy_id
                && loaded.resource_policy_revision == first.resource_policy_revision
                && loaded.credential_ref == provisioning.credential_ref
                && loaded.credential_revision == provisioning.credential_revision
                && loaded.credential_auth_sha256 == provisioning.credential_auth_sha256
                && loaded.max_concurrency == provisioning.max_concurrency,
            format!("runtime loader changed the provisioned profile: {loaded:?}"),
        )?;
        let counts: (i64, i64, i64, i64, i32) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles),
                   (SELECT allocated_count FROM executor_resource_policies
                    WHERE resource_policy_id = $1 AND revision = $2)
            "#,
        )
        .bind(first.resource_policy_id)
        .bind(first.resource_policy_revision)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1, 1, 1, 0),
            format!("exact replay changed the durable graph: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn disabled_identity_at_every_layer_remains_a_kill_switch() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        for layer in ["pool", "account", "policy", "profile"] {
            let provisioning = fixture(&format!("disabled-{layer}"));
            let provisioned = provision_codex_execution_profile(&database.pool, &provisioning)
                .await
                .map_err(debug_error)?;
            match layer {
                "pool" => {
                    sqlx::query(
                        "UPDATE provider_credential_pools SET state = 'disabled' WHERE credential_pool_id = $1",
                    )
                    .bind(provisioned.credential_pool_id)
                    .execute(&database.pool)
                    .await
                    .map_err(debug_error)?;
                }
                "account" => {
                    sqlx::query(
                        "UPDATE provider_accounts SET state = 'disabled' WHERE provider_account_id = $1",
                    )
                    .bind(provisioned.provider_account_id)
                    .execute(&database.pool)
                    .await
                    .map_err(debug_error)?;
                }
                "policy" => {
                    sqlx::query(
                        "UPDATE executor_resource_policies SET state = 'disabled' WHERE resource_policy_id = $1 AND revision = $2",
                    )
                    .bind(provisioned.resource_policy_id)
                    .bind(provisioned.resource_policy_revision)
                    .execute(&database.pool)
                    .await
                    .map_err(debug_error)?;
                }
                "profile" => {
                    sqlx::query(
                        "UPDATE provider_execution_profiles SET state = 'disabled' WHERE execution_profile_id = $1",
                    )
                    .bind(provisioned.execution_profile_id)
                    .execute(&database.pool)
                    .await
                    .map_err(debug_error)?;
                }
                _ => unreachable!(),
            }
            require(
                provision_codex_execution_profile(&database.pool, &provisioning).await
                    == Err(CodexProfileProvisioningError::Conflict),
                format!("disabled {layer} identity was implicitly re-enabled"),
            )?;
            let state: String = match layer {
                "pool" => sqlx::query_scalar(
                    "SELECT state FROM provider_credential_pools WHERE credential_pool_id = $1",
                )
                .bind(provisioned.credential_pool_id)
                .fetch_one(&database.pool)
                .await,
                "account" => sqlx::query_scalar(
                    "SELECT state FROM provider_accounts WHERE provider_account_id = $1",
                )
                .bind(provisioned.provider_account_id)
                .fetch_one(&database.pool)
                .await,
                "policy" => sqlx::query_scalar(
                    "SELECT state FROM executor_resource_policies WHERE resource_policy_id = $1 AND revision = $2",
                )
                .bind(provisioned.resource_policy_id)
                .bind(provisioned.resource_policy_revision)
                .fetch_one(&database.pool)
                .await,
                "profile" => sqlx::query_scalar(
                    "SELECT state FROM provider_execution_profiles WHERE execution_profile_id = $1",
                )
                .bind(provisioned.execution_profile_id)
                .fetch_one(&database.pool)
                .await,
                _ => unreachable!(),
            }
            .map_err(debug_error)?;
            require(
                state == "disabled",
                format!("disabled {layer} identity changed state to {state}"),
            )?;
        }
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn conflicting_credential_identity_rolls_back_without_partial_rows() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = fixture("conflict");
        let first = provision_codex_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        let mut conflicting = provisioning.clone();
        conflicting.credential_auth_sha256 = "b".repeat(64);
        require(
            provision_codex_execution_profile(&database.pool, &conflicting).await
                == Err(CodexProfileProvisioningError::Conflict),
            "credential digest drift did not fail closed",
        )?;
        let stored: (String, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT account.credential_auth_sha256,
                   (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles)
            FROM provider_accounts account
            WHERE account.provider_account_id = $1
            "#,
        )
        .bind(first.provider_account_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            stored == ("a".repeat(64), 1, 1, 1, 1),
            format!("conflicting provisioning leaked partial state: {stored:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_exact_provisioning_has_one_durable_identity_graph() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = fixture("concurrent");
        let mut tasks = Vec::new();
        for _ in 0..20 {
            let pool = database.pool.clone();
            let provisioning = provisioning.clone();
            tasks.push(tokio::spawn(async move {
                provision_codex_execution_profile(&pool, &provisioning).await
            }));
        }
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.map_err(debug_error)?.map_err(debug_error)?);
        }
        let first = results
            .first()
            .ok_or_else(|| "concurrent provisioning returned no result".to_string())?;
        require(
            results.iter().all(|result| result == first),
            format!("concurrent provisioning returned divergent identities: {results:?}"),
        )?;
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles)
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1, 1, 1),
            format!("concurrent provisioning duplicated durable rows: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_conflicting_provisioning_commits_one_complete_winner() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let first = fixture("concurrent-conflict");
        let mut second = first.clone();
        second.credential_auth_sha256 = "b".repeat(64);
        let first_task = {
            let pool = database.pool.clone();
            tokio::spawn(async move { provision_codex_execution_profile(&pool, &first).await })
        };
        let second_task = {
            let pool = database.pool.clone();
            tokio::spawn(async move { provision_codex_execution_profile(&pool, &second).await })
        };
        let results = [
            first_task.await.map_err(debug_error)?,
            second_task.await.map_err(debug_error)?,
        ];
        require(
            results.iter().filter(|result| result.is_ok()).count() == 1
                && results
                    .iter()
                    .filter(|result| {
                        result.as_ref().err() == Some(&CodexProfileProvisioningError::Conflict)
                    })
                    .count()
                    == 1,
            format!("concurrent conflicting provisioning did not choose one winner: {results:?}"),
        )?;
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles)
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1, 1, 1),
            format!("concurrent conflict produced a hybrid graph: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn late_profile_conflict_rolls_back_new_dependency_rows() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let existing = fixture("existing-profile");
        provision_codex_execution_profile(&database.pool, &existing)
            .await
            .map_err(debug_error)?;
        let mut conflicting = fixture("new-dependencies");
        conflicting.profile_key = existing.profile_key;
        require(
            provision_codex_execution_profile(&database.pool, &conflicting).await
                == Err(CodexProfileProvisioningError::Conflict),
            "late profile identity conflict did not fail closed",
        )?;
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles)
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1, 1, 1),
            format!("late profile conflict leaked dependency rows: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

fn fixture(suffix: &str) -> CodexExecutionProfileProvisioning {
    CodexExecutionProfileProvisioning {
        profile_key: format!("openai-codex-generation-v1-{suffix}"),
        credential_pool_key: format!("openai-codex-pool-{suffix}"),
        provider_account_key: format!("openai-codex-account-{suffix}"),
        credential_ref: format!("mounted.openai-codex.{suffix}.1"),
        credential_revision: 1,
        credential_auth_sha256: "a".repeat(64),
        max_concurrency: 3,
    }
}

fn grok_fixture(suffix: &str) -> GrokExecutionProfileProvisioning {
    GrokExecutionProfileProvisioning {
        profile_key: format!("grok-cli-generation-v1-{suffix}"),
        credential_pool_key: format!("grok-cli-pool-{suffix}"),
        provider_account_key: format!("grok-cli-account-{suffix}"),
        credential_ref: format!("mounted.grok-cli.{suffix}.1"),
        credential_revision: 1,
        credential_auth_sha256: "a".repeat(64),
        max_concurrency: 1,
    }
}

fn dreamina_fixture(suffix: &str) -> DreaminaExecutionProfileProvisioning {
    DreaminaExecutionProfileProvisioning {
        profile_key: format!("dreamina-cli-generation-v1-{suffix}"),
        credential_pool_key: format!("dreamina-cli-pool-{suffix}"),
        provider_account_key: format!("dreamina-cli-account-{suffix}"),
        credential_ref: format!("managed.dreamina-cli.{suffix}.1"),
        credential_revision: 1,
        credential_auth_sha256: "c".repeat(64),
        max_concurrency: 1,
    }
}

fn dreamina_video_fixture(suffix: &str) -> DreaminaExecutionProfileProvisioning {
    DreaminaExecutionProfileProvisioning {
        profile_key: format!("dreamina-cli-video-v1-{suffix}"),
        credential_pool_key: format!("dreamina-cli-video-pool-{suffix}"),
        provider_account_key: format!("dreamina-cli-video-account-{suffix}"),
        credential_ref: format!("managed.dreamina-cli-video.{suffix}.1"),
        credential_revision: 1,
        credential_auth_sha256: "d".repeat(64),
        max_concurrency: 1,
    }
}

fn grok_video_fixture(suffix: &str) -> GrokExecutionProfileProvisioning {
    GrokExecutionProfileProvisioning {
        profile_key: format!("grok-cli-video-v1-{suffix}"),
        credential_pool_key: format!("grok-cli-video-pool-{suffix}"),
        provider_account_key: format!("grok-cli-video-account-{suffix}"),
        credential_ref: format!("mounted.grok-cli-video.{suffix}.1"),
        credential_revision: 1,
        credential_auth_sha256: "b".repeat(64),
        max_concurrency: 1,
    }
}

struct TestDatabase {
    schema: String,
    pool: PgPool,
}

impl TestDatabase {
    async fn new() -> TestResult<Option<Self>> {
        let Some(url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping PostgreSQL provisioning test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_provisioning_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 16, &schema)
            .await
            .map_err(debug_error)?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        if let Err(error) = run_migrations(&pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return Err(format!("migration failed: {error:?}"));
        }
        Ok(Some(Self { schema, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(debug_error);
        self.pool.close().await;
        result.map(|_| ())
    }
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn combine(primary: TestResult, cleanup: TestResult) -> TestResult {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(format!("{primary}; cleanup also failed: {cleanup}")),
    }
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}
