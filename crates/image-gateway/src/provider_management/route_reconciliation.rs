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
                )
            )
            OR NOT EXISTS (
                SELECT 1 FROM provider_route_members member
                WHERE member.route_id = head.route_id
                  AND member.route_revision = head.current_revision
                  AND member.state = 'enabled'
            )
          )
        ORDER BY head.route_id
        "#,
    )
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
    if !has_stale_member && members.iter().any(|member| member.state == "enabled") {
        tx.commit().await.map_err(store_unavailable)?;
        return Ok(RouteReconciliationOutcome::Skipped);
    }
    if !has_stale_member {
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
                 AND profile.command_schema = member.command_schema,
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
    .fetch_all(&mut **tx)
    .await
    .map_err(store_unavailable)
}

async fn compatible_replacements(
    tx: &mut Transaction<'_, Postgres>,
    route: &RouteRevision,
    member: &RouteMember,
) -> Result<Vec<Uuid>, ImageGatewayError> {
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
        ORDER BY profile.created_at_ms DESC, profile.execution_profile_id
        FOR SHARE OF profile
        "#,
    )
    .bind(member.provider_account_id)
    .bind(&route.provider_id)
    .bind(&route.operation_id)
    .bind(&route.command_schema)
    .bind(member.execution_profile_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(store_unavailable)
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
               api_profile, public_model_id, provider_model_id, execution_model_id,
               media_kind, $4
        FROM provider_route_model_mappings
        WHERE route_id = $1 AND route_revision = $2
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

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ImageGatewayError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(store_unavailable)
}

fn store_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("provider route reconciliation is unavailable")
}
