use image_provider_contracts::openai_codex;
use image_provider_grok_cli::{
    ADAPTER_REVISION as GROK_ADAPTER_REVISION, GROK_IMAGE_EDIT_COMMAND_SCHEMA,
    GROK_IMAGE_EDIT_OPERATION_V1, GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
    GROK_IMAGE_GENERATION_OPERATION_V1, GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
    GROK_VIDEO_GENERATION_OPERATION_V1, PROVIDER_ID as GROK_PROVIDER_ID,
    VIDEO_ADAPTER_REVISION as GROK_VIDEO_ADAPTER_REVISION,
};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::ImageGatewayError;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutionProfileRouteReconciliationReport {
    pub inspected_routes: u64,
    pub revised_routes: u64,
    pub unresolved_routes: u64,
    pub api_key_bindings_moved: u64,
    pub project_bindings_moved: u64,
    pub platform_bindings_moved: u64,
}

#[derive(sqlx::FromRow)]
struct RouteRevision {
    route_id: Uuid,
    revision: i64,
    provider_id: String,
    operation_id: String,
    command_schema: String,
}

#[derive(sqlx::FromRow)]
struct RouteMember {
    provider_account_id: Uuid,
    execution_profile_id: Uuid,
    priority: i16,
    weight: i32,
    state: String,
    minimum_remaining_percent: i16,
    profile_state: String,
    profile_is_compatible: bool,
}

#[derive(Clone, Debug)]
struct ExpectedRuntimeBinding {
    operation_descriptor_sha256_v1: String,
    adapter_revision: &'static str,
}

struct ReconciledRoute {
    api_key_bindings_moved: u64,
    project_bindings_moved: u64,
    platform_bindings_moved: u64,
}

enum RouteReconciliationOutcome {
    Revised(ReconciledRoute),
    Unresolved,
    Skipped,
}

pub async fn reconcile_execution_profile_routes(
    pool: &PgPool,
) -> Result<ExecutionProfileRouteReconciliationReport, ImageGatewayError> {
    let route_ids = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT head.route_id
        FROM provider_route_heads head
        WHERE head.state = 'enabled'
          AND (
            EXISTS (
              SELECT 1
              FROM provider_route_members member
              LEFT JOIN provider_execution_profiles profile
                ON profile.execution_profile_id = member.execution_profile_id
              WHERE member.route_id = head.route_id
                AND member.route_revision = head.current_revision
                AND member.state = 'enabled'
                AND (
                  profile.execution_profile_id IS NULL
                  OR profile.state <> 'enabled'
                  OR profile.provider_account_id <> member.provider_account_id
                  OR profile.provider_id <> member.provider_id
                  OR profile.operation_id <> member.operation_id
                  OR profile.command_schema <> member.command_schema
                  OR (head.provider_id = $4 AND (
                    (
                      head.operation_id = $5
                      AND head.command_schema = $6
                      AND (
                        profile.operation_descriptor_sha256_v1 <> $7
                        OR profile.adapter_revision <> $8
                      )
                    )
                    OR (
                      head.operation_id = $9
                      AND head.command_schema = $10
                      AND (
                        profile.operation_descriptor_sha256_v1 <> $11
                        OR profile.adapter_revision <> $12
                      )
                    )
                    OR (
                      head.operation_id = $13
                      AND head.command_schema = $14
                      AND (
                        profile.operation_descriptor_sha256_v1 <> $15
                        OR profile.adapter_revision <> $16
                      )
                    )
                  ))
                )
            )
            OR NOT EXISTS (
                SELECT 1 FROM provider_route_members member
                WHERE member.route_id = head.route_id
                  AND member.route_revision = head.current_revision
                  AND member.state = 'enabled'
            )
            OR EXISTS (
                SELECT 1
                FROM provider_route_model_mappings mapping
                WHERE mapping.route_id = head.route_id
                  AND mapping.route_revision = head.current_revision
                  AND mapping.provider_id = $1
                  AND mapping.execution_model_id = $2
                  AND mapping.provider_model_id <> $3
            )
          )
        ORDER BY head.route_id
        "#,
    )
    .bind(openai_codex::PROVIDER_ID)
    .bind(openai_codex::MODEL_GPT_IMAGE_2_SNAPSHOT)
    .bind(openai_codex::MODEL_GPT_IMAGE_2)
    .bind(GROK_PROVIDER_ID)
    .bind(GROK_VIDEO_GENERATION_OPERATION_V1.id)
    .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA)
    .bind(GROK_VIDEO_GENERATION_OPERATION_V1.canonical_sha256_v1_hex())
    .bind(GROK_VIDEO_ADAPTER_REVISION)
    .bind(GROK_IMAGE_GENERATION_OPERATION_V1.id)
    .bind(GROK_IMAGE_GENERATION_COMMAND_SCHEMA)
    .bind(GROK_IMAGE_GENERATION_OPERATION_V1.canonical_sha256_v1_hex())
    .bind(GROK_ADAPTER_REVISION)
    .bind(GROK_IMAGE_EDIT_OPERATION_V1.id)
    .bind(GROK_IMAGE_EDIT_COMMAND_SCHEMA)
    .bind(GROK_IMAGE_EDIT_OPERATION_V1.canonical_sha256_v1_hex())
    .bind(GROK_ADAPTER_REVISION)
    .fetch_all(pool)
    .await
    .map_err(store_unavailable)?;

    let mut report = ExecutionProfileRouteReconciliationReport {
        inspected_routes: route_ids.len() as u64,
        ..ExecutionProfileRouteReconciliationReport::default()
    };
    for route_id in route_ids {
        match reconcile_route(pool, route_id).await? {
            RouteReconciliationOutcome::Revised(reconciled) => {
                report.revised_routes += 1;
                report.api_key_bindings_moved += reconciled.api_key_bindings_moved;
                report.project_bindings_moved += reconciled.project_bindings_moved;
                report.platform_bindings_moved += reconciled.platform_bindings_moved;
            }
            RouteReconciliationOutcome::Unresolved => report.unresolved_routes += 1,
            RouteReconciliationOutcome::Skipped => {}
        }
    }

    if report.unresolved_routes > 0 {
        tracing::warn!(
            inspected_routes = report.inspected_routes,
            revised_routes = report.revised_routes,
            unresolved_routes = report.unresolved_routes,
            "enabled provider routes reference disabled or incompatible execution profiles"
        );
    }
    if report.revised_routes > 0 {
        tracing::info!(
            revised_routes = report.revised_routes,
            api_key_bindings_moved = report.api_key_bindings_moved,
            project_bindings_moved = report.project_bindings_moved,
            platform_bindings_moved = report.platform_bindings_moved,
            "provider routes advanced to replacement execution profiles"
        );
    }
    Ok(report)
}

async fn reconcile_route(
    pool: &PgPool,
    route_id: Uuid,
) -> Result<RouteReconciliationOutcome, ImageGatewayError> {
    let mut tx = pool.begin().await.map_err(store_unavailable)?;
    let Some(route) = current_route_for_update(&mut tx, route_id).await? else {
        tx.commit().await.map_err(store_unavailable)?;
        return Ok(RouteReconciliationOutcome::Skipped);
    };
    let members = route_members(&mut tx, &route).await?;
    let needs_model_reconciliation = route_needs_model_reconciliation(&mut tx, &route).await?;
    let mut replacements = Vec::new();
    let mut has_stale_member = false;

    for member in members.iter().filter(|member| member.state == "enabled") {
        if member.profile_state == "enabled" && member.profile_is_compatible {
            continue;
        }
        has_stale_member = true;
        let candidates = compatible_replacements(&mut tx, &route, member).await?;
        if candidates.len() != 1 {
            tracing::warn!(
                route_id = %route.route_id,
                route_revision = route.revision,
                provider_account_id = %member.provider_account_id,
                execution_profile_id = %member.execution_profile_id,
                replacement_candidates = candidates.len(),
                "provider route execution profile replacement is unresolved"
            );
            tx.commit().await.map_err(store_unavailable)?;
            return Ok(RouteReconciliationOutcome::Unresolved);
        }
        replacements.push((member.execution_profile_id, candidates[0]));
    }
    if !has_stale_member
        && !needs_model_reconciliation
        && members.iter().any(|member| member.state == "enabled")
    {
        tx.commit().await.map_err(store_unavailable)?;
        return Ok(RouteReconciliationOutcome::Skipped);
    }
    if !members.iter().any(|member| member.state == "enabled") {
        tracing::warn!(
            route_id = %route.route_id,
            route_revision = route.revision,
            "enabled provider route has no enabled members"
        );
        tx.commit().await.map_err(store_unavailable)?;
        return Ok(RouteReconciliationOutcome::Unresolved);
    }

    let next_revision = route.revision.checked_add(1).ok_or_else(|| {
        ImageGatewayError::service_unavailable("provider route revision exhausted")
    })?;
    let now = database_now(&mut tx).await?;
    insert_route_revision(&mut tx, &route, next_revision, now).await?;
    insert_route_members(&mut tx, &route, next_revision, now, &members, &replacements).await?;
    copy_route_model_mappings(&mut tx, &route, next_revision, now).await?;

    let updated = sqlx::query(
        r#"
        UPDATE provider_route_heads
        SET current_revision = $3, updated_at_ms = $4
        WHERE route_id = $1 AND current_revision = $2 AND state = 'enabled'
        "#,
    )
    .bind(route.route_id)
    .bind(route.revision)
    .bind(next_revision)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(store_unavailable)?;
    if updated.rows_affected() != 1 {
        return Err(ImageGatewayError::service_unavailable(
            "provider route changed during execution profile reconciliation",
        ));
    }

    let api_key_ids = sqlx::query_scalar::<_, String>(
        r#"
        UPDATE gateway_api_key_provider_routes
        SET route_revision = $3, bound_at_ms = $4
        WHERE route_id = $1 AND route_revision = $2
        RETURNING api_key_id
        "#,
    )
    .bind(route.route_id)
    .bind(route.revision)
    .bind(next_revision)
    .bind(now)
    .fetch_all(&mut *tx)
    .await
    .map_err(store_unavailable)?;
    if !api_key_ids.is_empty() {
        sqlx::query(
            "UPDATE gateway_api_keys SET authz_version = authz_version + 1 WHERE id = ANY($1)",
        )
        .bind(&api_key_ids)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
    }
    let project_bindings_moved = sqlx::query(
        r#"
        UPDATE gateway_project_provider_routes
        SET route_revision = $3, updated_at_ms = $4
        WHERE route_id = $1 AND route_revision = $2
        "#,
    )
    .bind(route.route_id)
    .bind(route.revision)
    .bind(next_revision)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(store_unavailable)?
    .rows_affected();
    let platform_bindings_moved = sqlx::query(
        r#"
        UPDATE gateway_platform_provider_routes
        SET route_revision = $3, updated_at_ms = $4
        WHERE route_id = $1 AND route_revision = $2
        "#,
    )
    .bind(route.route_id)
    .bind(route.revision)
    .bind(next_revision)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(store_unavailable)?
    .rows_affected();

    tx.commit().await.map_err(store_unavailable)?;
    Ok(RouteReconciliationOutcome::Revised(ReconciledRoute {
        api_key_bindings_moved: api_key_ids.len() as u64,
        project_bindings_moved,
        platform_bindings_moved,
    }))
}

async fn current_route_for_update(
    tx: &mut Transaction<'_, Postgres>,
    route_id: Uuid,
) -> Result<Option<RouteRevision>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT route.route_id, route.revision, route.provider_id,
               route.operation_id, route.command_schema
        FROM provider_route_heads head
        JOIN provider_routes route
          ON route.route_id = head.route_id
         AND route.revision = head.current_revision
         AND route.provider_id = head.provider_id
         AND route.operation_id = head.operation_id
         AND route.command_schema = head.command_schema
        WHERE head.route_id = $1 AND head.state = 'enabled'
        FOR UPDATE OF head
        "#,
    )
    .bind(route_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(store_unavailable)
}

async fn route_members(
    tx: &mut Transaction<'_, Postgres>,
    route: &RouteRevision,
) -> Result<Vec<RouteMember>, ImageGatewayError> {
    let expected = expected_runtime_binding(route);
    sqlx::query_as(
        r#"
        SELECT member.provider_account_id, member.execution_profile_id,
               member.priority, member.weight, member.state,
               member.minimum_remaining_percent,
               COALESCE(profile.state, 'missing') AS profile_state,
               COALESCE(
                 profile.provider_account_id = member.provider_account_id
                 AND profile.provider_id = member.provider_id
                 AND profile.operation_id = member.operation_id
                 AND profile.command_schema = member.command_schema
                 AND (
                   $3::TEXT IS NULL
                   OR (
                     profile.operation_descriptor_sha256_v1 = $3
                     AND profile.adapter_revision = $4
                   )
                 ),
                 FALSE
               ) AS profile_is_compatible
        FROM provider_route_members member
        LEFT JOIN provider_execution_profiles profile
          ON profile.execution_profile_id = member.execution_profile_id
        WHERE member.route_id = $1 AND member.route_revision = $2
        ORDER BY member.provider_account_id, member.execution_profile_id
        "#,
    )
    .bind(route.route_id)
    .bind(route.revision)
    .bind(
        expected
            .as_ref()
            .map(|binding| binding.operation_descriptor_sha256_v1.as_str()),
    )
    .bind(expected.as_ref().map(|binding| binding.adapter_revision))
    .fetch_all(&mut **tx)
    .await
    .map_err(store_unavailable)
}

async fn route_needs_model_reconciliation(
    tx: &mut Transaction<'_, Postgres>,
    route: &RouteRevision,
) -> Result<bool, ImageGatewayError> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM provider_route_model_mappings mapping
          WHERE mapping.route_id = $1
            AND mapping.route_revision = $2
            AND mapping.provider_id = $3
            AND mapping.execution_model_id = $4
            AND mapping.provider_model_id <> $5
        )
        "#,
    )
    .bind(route.route_id)
    .bind(route.revision)
    .bind(openai_codex::PROVIDER_ID)
    .bind(openai_codex::MODEL_GPT_IMAGE_2_SNAPSHOT)
    .bind(openai_codex::MODEL_GPT_IMAGE_2)
    .fetch_one(&mut **tx)
    .await
    .map_err(store_unavailable)
}

async fn compatible_replacements(
    tx: &mut Transaction<'_, Postgres>,
    route: &RouteRevision,
    member: &RouteMember,
) -> Result<Vec<Uuid>, ImageGatewayError> {
    let expected = expected_runtime_binding(route);
    sqlx::query_scalar(
        r#"
        SELECT profile.execution_profile_id
        FROM provider_execution_profiles profile
        WHERE profile.provider_account_id = $1
          AND profile.provider_id = $2
          AND profile.operation_id = $3
          AND profile.command_schema = $4
          AND profile.state = 'enabled'
          AND profile.execution_profile_id <> $5
          AND (
            $6::TEXT IS NULL
            OR (
              profile.operation_descriptor_sha256_v1 = $6
              AND profile.adapter_revision = $7
            )
          )
        ORDER BY profile.created_at_ms DESC, profile.execution_profile_id
        FOR SHARE OF profile
        "#,
    )
    .bind(member.provider_account_id)
    .bind(&route.provider_id)
    .bind(&route.operation_id)
    .bind(&route.command_schema)
    .bind(member.execution_profile_id)
    .bind(
        expected
            .as_ref()
            .map(|binding| binding.operation_descriptor_sha256_v1.as_str()),
    )
    .bind(expected.as_ref().map(|binding| binding.adapter_revision))
    .fetch_all(&mut **tx)
    .await
    .map_err(store_unavailable)
}

fn expected_runtime_binding(route: &RouteRevision) -> Option<ExpectedRuntimeBinding> {
    if route.provider_id != GROK_PROVIDER_ID {
        return None;
    }
    match (route.operation_id.as_str(), route.command_schema.as_str()) {
        (operation, schema)
            if operation == GROK_VIDEO_GENERATION_OPERATION_V1.id
                && schema == GROK_VIDEO_GENERATION_COMMAND_SCHEMA =>
        {
            Some(ExpectedRuntimeBinding {
                operation_descriptor_sha256_v1: GROK_VIDEO_GENERATION_OPERATION_V1
                    .canonical_sha256_v1_hex(),
                adapter_revision: GROK_VIDEO_ADAPTER_REVISION,
            })
        }
        (operation, schema)
            if operation == GROK_IMAGE_GENERATION_OPERATION_V1.id
                && schema == GROK_IMAGE_GENERATION_COMMAND_SCHEMA =>
        {
            Some(ExpectedRuntimeBinding {
                operation_descriptor_sha256_v1: GROK_IMAGE_GENERATION_OPERATION_V1
                    .canonical_sha256_v1_hex(),
                adapter_revision: GROK_ADAPTER_REVISION,
            })
        }
        (operation, schema)
            if operation == GROK_IMAGE_EDIT_OPERATION_V1.id
                && schema == GROK_IMAGE_EDIT_COMMAND_SCHEMA =>
        {
            Some(ExpectedRuntimeBinding {
                operation_descriptor_sha256_v1: GROK_IMAGE_EDIT_OPERATION_V1
                    .canonical_sha256_v1_hex(),
                adapter_revision: GROK_ADAPTER_REVISION,
            })
        }
        _ => None,
    }
}

async fn insert_route_revision(
    tx: &mut Transaction<'_, Postgres>,
    route: &RouteRevision,
    next_revision: i64,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO provider_routes
          (route_id, revision, route_key, display_name, provider_id, operation_id,
           command_schema, route_kind, selection_strategy, quota_freshness_ms,
           unknown_quota_policy, state, created_at_ms)
        SELECT route_id, $3, route_key, display_name, provider_id, operation_id,
               command_schema, route_kind, selection_strategy, quota_freshness_ms,
               unknown_quota_policy, state, $4
        FROM provider_routes
        WHERE route_id = $1 AND revision = $2
        "#,
    )
    .bind(route.route_id)
    .bind(route.revision)
    .bind(next_revision)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    Ok(())
}

async fn insert_route_members(
    tx: &mut Transaction<'_, Postgres>,
    route: &RouteRevision,
    next_revision: i64,
    now: i64,
    members: &[RouteMember],
    replacements: &[(Uuid, Uuid)],
) -> Result<(), ImageGatewayError> {
    for member in members {
        let execution_profile_id = replacements
            .iter()
            .find_map(|(old, new)| (*old == member.execution_profile_id).then_some(*new))
            .unwrap_or(member.execution_profile_id);
        sqlx::query(
            r#"
            INSERT INTO provider_route_members
              (route_id, route_revision, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, priority, weight, state,
               minimum_remaining_percent, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(route.route_id)
        .bind(next_revision)
        .bind(&route.provider_id)
        .bind(&route.operation_id)
        .bind(&route.command_schema)
        .bind(member.provider_account_id)
        .bind(execution_profile_id)
        .bind(member.priority)
        .bind(member.weight)
        .bind(&member.state)
        .bind(member.minimum_remaining_percent)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(store_unavailable)?;
    }
    Ok(())
}

async fn copy_route_model_mappings(
    tx: &mut Transaction<'_, Postgres>,
    route: &RouteRevision,
    next_revision: i64,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings
          (route_id, route_revision, provider_id, operation_id, command_schema,
           api_profile, public_model_id, provider_model_id, execution_model_id,
           media_kind, created_at_ms)
        SELECT route_id, $3, provider_id, operation_id, command_schema,
               api_profile, public_model_id,
               CASE
                 WHEN provider_id = $5
                  AND execution_model_id = $6
                   THEN $7
                 ELSE provider_model_id
               END,
               execution_model_id,
               media_kind, $4
        FROM provider_route_model_mappings
        WHERE route_id = $1 AND route_revision = $2
        "#,
    )
    .bind(route.route_id)
    .bind(route.revision)
    .bind(next_revision)
    .bind(now)
    .bind(openai_codex::PROVIDER_ID)
    .bind(openai_codex::MODEL_GPT_IMAGE_2_SNAPSHOT)
    .bind(openai_codex::MODEL_GPT_IMAGE_2)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    Ok(())
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ImageGatewayError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(store_unavailable)
}

fn store_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("provider route reconciliation is unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(operation_id: &str, command_schema: &str) -> RouteRevision {
        RouteRevision {
            route_id: Uuid::nil(),
            revision: 1,
            provider_id: GROK_PROVIDER_ID.to_owned(),
            operation_id: operation_id.to_owned(),
            command_schema: command_schema.to_owned(),
        }
    }

    #[test]
    fn grok_routes_require_the_operation_specific_current_adapter() {
        let image = expected_runtime_binding(&route(
            GROK_IMAGE_GENERATION_OPERATION_V1.id,
            GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
        ))
        .unwrap();
        assert_eq!(image.adapter_revision, GROK_ADAPTER_REVISION);

        let edit = expected_runtime_binding(&route(
            GROK_IMAGE_EDIT_OPERATION_V1.id,
            GROK_IMAGE_EDIT_COMMAND_SCHEMA,
        ))
        .unwrap();
        assert_eq!(edit.adapter_revision, GROK_ADAPTER_REVISION);

        let video = expected_runtime_binding(&route(
            GROK_VIDEO_GENERATION_OPERATION_V1.id,
            GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
        ))
        .unwrap();
        assert_eq!(video.adapter_revision, GROK_VIDEO_ADAPTER_REVISION);
    }
}
