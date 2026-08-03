use async_trait::async_trait;
use sqlx::PgPool;
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::ImageGatewayError;

use super::{ModelRoutingStore, PublicModelRoute, ResolvedModelRoute};

#[derive(Clone)]
pub struct PostgresModelRoutingStore {
    pool: PgPool,
}

#[derive(sqlx::FromRow)]
struct ResolvedModelRouteRow {
    public_model_id: String,
    api_profile: String,
    provider_id: String,
    operation_id: String,
    command_schema: String,
    provider_model_id: String,
    execution_model_id: String,
    media_kind: String,
    route_id: Uuid,
    route_revision: i64,
}

#[derive(sqlx::FromRow)]
struct ConsoleModelRouteRow {
    public_model_id: String,
    api_profile: String,
    provider_id: String,
    operation_id: String,
    command_schema: String,
    provider_model_id: String,
    execution_model_id: String,
    media_kind: String,
    route_id: Uuid,
    route_revision: i64,
    created_at_ms: i64,
}

impl PostgresModelRoutingStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn ensure_api_key_version(
        &self,
        project_id: &str,
        api_key_id: &str,
        credential_authz_version: i64,
    ) -> Result<(), ImageGatewayError> {
        let current: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT authz_version
            FROM gateway_api_keys
            WHERE id = $1 AND project_id = $2 AND deleted_at IS NULL
              AND (expires_at IS NULL OR expires_at > EXTRACT(EPOCH FROM clock_timestamp())::BIGINT)
            "#,
        )
        .bind(api_key_id)
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_unavailable)?;
        if current == Some(credential_authz_version) {
            Ok(())
        } else {
            Err(ImageGatewayError::authentication())
        }
    }

    async fn model_is_allowed(
        &self,
        project_id: &str,
        operation_id: &str,
        api_profile: &str,
        public_model_id: &str,
        media_kind: &str,
    ) -> Result<bool, ImageGatewayError> {
        sqlx::query_scalar(
            r#"
            SELECT
              NOT EXISTS(
                SELECT 1
                FROM project_model_policies
                WHERE project_id = $1
              )
              OR EXISTS(
                SELECT 1
                FROM project_model_access_entries
                WHERE project_id = $1
                  AND operation_id = $2
                  AND api_profile = $3
                  AND public_model_id = $4
                  AND media_kind = $5
              )
            "#,
        )
        .bind(project_id)
        .bind(operation_id)
        .bind(api_profile)
        .bind(public_model_id)
        .bind(media_kind)
        .fetch_one(&self.pool)
        .await
        .map_err(store_unavailable)
    }

    async fn ensure_model_allowed(
        &self,
        project_id: &str,
        route: ResolvedModelRoute,
    ) -> Result<ResolvedModelRoute, ImageGatewayError> {
        if self
            .model_is_allowed(
                project_id,
                &route.operation_id,
                &route.api_profile,
                &route.public_model_id,
                &route.media_kind,
            )
            .await?
        {
            Ok(route)
        } else {
            Err(ImageGatewayError::model_not_found(&route.public_model_id))
        }
    }

    async fn filter_allowed_models(
        &self,
        project_id: &str,
        models: Vec<PublicModelRoute>,
    ) -> Result<Vec<PublicModelRoute>, ImageGatewayError> {
        let configured: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM project_model_policies WHERE project_id = $1)",
        )
        .bind(project_id)
        .fetch_one(&self.pool)
        .await
        .map_err(store_unavailable)?;
        if !configured {
            return Ok(models);
        }
        let allowed = sqlx::query_as::<_, AllowedModelKey>(
            r#"
            SELECT operation_id, api_profile, public_model_id, media_kind
            FROM project_model_access_entries
            WHERE project_id = $1
            "#,
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
        .map_err(store_unavailable)?
        .into_iter()
        .map(|row| {
            (
                row.operation_id,
                row.api_profile,
                row.public_model_id,
                row.media_kind,
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
        Ok(models
            .into_iter()
            .filter(|model| {
                allowed.contains(&(
                    model.operation_id.clone(),
                    model.api_profile.clone(),
                    model.id.clone(),
                    model.media_kind.clone(),
                ))
            })
            .collect())
    }
}

#[derive(sqlx::FromRow)]
struct AllowedModelKey {
    operation_id: String,
    api_profile: String,
    public_model_id: String,
    media_kind: String,
}

#[async_trait]
impl ModelRoutingStore for PostgresModelRoutingStore {
    async fn list_api_key_models(
        &self,
        project_id: &str,
        api_key_id: &str,
        credential_authz_version: i64,
    ) -> Result<Vec<PublicModelRoute>, ImageGatewayError> {
        self.ensure_api_key_version(project_id, api_key_id, credential_authz_version)
            .await?;
        let models = sqlx::query_as::<_, PublicModelRoute>(
            r#"
            SELECT mapping.public_model_id AS id, mapping.provider_model_id,
                   mapping.api_profile,
                   mapping.provider_id, mapping.operation_id, mapping.media_kind,
                   mapping.created_at_ms
            FROM gateway_api_key_provider_routes binding
            JOIN gateway_api_keys api_key
              ON api_key.id = binding.api_key_id
             AND api_key.project_id = binding.project_id
             AND api_key.authz_version = $3
             AND api_key.deleted_at IS NULL
            JOIN provider_route_heads head
              ON head.route_id = binding.route_id AND head.state = 'enabled'
            JOIN provider_route_model_mappings mapping
              ON mapping.route_id = binding.route_id
             AND mapping.route_revision = binding.route_revision
             AND mapping.provider_id = binding.provider_id
             AND mapping.operation_id = binding.operation_id
             AND mapping.command_schema = binding.command_schema
            WHERE binding.api_key_id = $1 AND binding.project_id = $2
              AND EXISTS (
                SELECT 1
                FROM provider_route_members member
                JOIN provider_execution_profiles profile
                  ON profile.execution_profile_id = member.execution_profile_id
                 AND profile.provider_account_id = member.provider_account_id
                 AND profile.provider_id = member.provider_id
                 AND profile.operation_id = member.operation_id
                 AND profile.command_schema = member.command_schema
                LEFT JOIN provider_account_model_configurations configuration
                  ON configuration.provider_account_id = member.provider_account_id
                 AND configuration.provider_id = member.provider_id
                WHERE member.route_id = mapping.route_id
                  AND member.route_revision = mapping.route_revision
                  AND member.state = 'enabled' AND profile.state = 'enabled'
                  AND (
                    configuration.provider_account_id IS NULL
                    OR configuration.mode = 'automatic'
                    OR EXISTS (
                      SELECT 1 FROM provider_account_model_bindings account_model
                      WHERE account_model.provider_account_id = member.provider_account_id
                        AND account_model.provider_id = member.provider_id
                        AND account_model.model_id = mapping.provider_model_id
                        AND account_model.media_kind = mapping.media_kind
                    )
                  )
              )
            ORDER BY mapping.public_model_id, mapping.api_profile, mapping.provider_id
            "#,
        )
        .bind(api_key_id)
        .bind(project_id)
        .bind(credential_authz_version)
        .fetch_all(&self.pool)
        .await
        .map_err(store_unavailable)?;
        if models.is_empty() {
            self.warn_if_api_key_routes_have_no_active_profiles(project_id, api_key_id)
                .await?;
        }
        self.ensure_api_key_version(project_id, api_key_id, credential_authz_version)
            .await?;
        self.filter_allowed_models(project_id, models).await
    }

    async fn resolve_api_key_model(
        &self,
        project_id: &str,
        api_key_id: &str,
        credential_authz_version: i64,
        provider_id: &str,
        operation_id: &str,
        api_profile: &str,
        requested_public_model_id: Option<&str>,
        default_provider_model_id: &str,
    ) -> Result<Option<ResolvedModelRoute>, ImageGatewayError> {
        self.ensure_api_key_version(project_id, api_key_id, credential_authz_version)
            .await?;
        let row = sqlx::query_as::<_, ResolvedModelRouteRow>(
            r#"
            SELECT mapping.public_model_id, mapping.api_profile,
                   mapping.provider_id, mapping.operation_id, mapping.command_schema,
                   mapping.provider_model_id,
                   mapping.execution_model_id, mapping.media_kind,
                   mapping.route_id, mapping.route_revision
            FROM gateway_api_key_provider_routes binding
            JOIN gateway_api_keys api_key
              ON api_key.id = binding.api_key_id
             AND api_key.project_id = binding.project_id
             AND api_key.authz_version = $8
             AND api_key.deleted_at IS NULL
            JOIN provider_route_heads head
              ON head.route_id = binding.route_id AND head.state = 'enabled'
            JOIN provider_route_model_mappings mapping
              ON mapping.route_id = binding.route_id
             AND mapping.route_revision = binding.route_revision
             AND mapping.provider_id = binding.provider_id
             AND mapping.operation_id = binding.operation_id
             AND mapping.command_schema = binding.command_schema
            WHERE binding.api_key_id = $1 AND binding.project_id = $2
              AND binding.provider_id = $3 AND binding.operation_id = $4
              AND mapping.api_profile = $5
              AND (
                ($6::TEXT IS NOT NULL AND mapping.public_model_id = $6)
                OR ($6::TEXT IS NULL AND mapping.provider_model_id = $7)
              )
              AND EXISTS (
                SELECT 1
                FROM provider_route_members member
                JOIN provider_execution_profiles profile
                  ON profile.execution_profile_id = member.execution_profile_id
                 AND profile.provider_account_id = member.provider_account_id
                 AND profile.provider_id = member.provider_id
                 AND profile.operation_id = member.operation_id
                 AND profile.command_schema = member.command_schema
                LEFT JOIN provider_account_model_configurations configuration
                  ON configuration.provider_account_id = member.provider_account_id
                 AND configuration.provider_id = member.provider_id
                WHERE member.route_id = mapping.route_id
                  AND member.route_revision = mapping.route_revision
                  AND member.state = 'enabled' AND profile.state = 'enabled'
                  AND (
                    configuration.provider_account_id IS NULL
                    OR configuration.mode = 'automatic'
                    OR EXISTS (
                      SELECT 1 FROM provider_account_model_bindings account_model
                      WHERE account_model.provider_account_id = member.provider_account_id
                        AND account_model.provider_id = member.provider_id
                        AND account_model.model_id = mapping.provider_model_id
                        AND account_model.media_kind = mapping.media_kind
                    )
                  )
              )
            "#,
        )
        .bind(api_key_id)
        .bind(project_id)
        .bind(provider_id)
        .bind(operation_id)
        .bind(api_profile)
        .bind(requested_public_model_id)
        .bind(default_provider_model_id)
        .bind(credential_authz_version)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_unavailable)?;
        if let Some(row) = row {
            return self
                .ensure_model_allowed(
                    project_id,
                    ResolvedModelRoute {
                        public_model_id: row.public_model_id,
                        api_profile: row.api_profile,
                        provider_id: row.provider_id,
                        operation_id: row.operation_id,
                        command_schema: row.command_schema,
                        provider_model_id: row.provider_model_id,
                        execution_model_id: row.execution_model_id,
                        media_kind: row.media_kind,
                        route_id: row.route_id,
                        route_revision: row.route_revision,
                    },
                )
                .await
                .map(Some);
        }

        let bound: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
              SELECT 1
              FROM gateway_api_key_provider_routes binding
              JOIN gateway_api_keys api_key
                ON api_key.id = binding.api_key_id
               AND api_key.project_id = binding.project_id
               AND api_key.authz_version = $3
               AND api_key.deleted_at IS NULL
              JOIN provider_route_heads head
                ON head.route_id = binding.route_id AND head.state = 'enabled'
              WHERE binding.api_key_id = $1 AND binding.project_id = $2
            )
            "#,
        )
        .bind(api_key_id)
        .bind(project_id)
        .bind(credential_authz_version)
        .fetch_one(&self.pool)
        .await
        .map_err(store_unavailable)?;
        self.ensure_api_key_version(project_id, api_key_id, credential_authz_version)
            .await?;
        if bound {
            Err(ImageGatewayError::model_not_found(
                requested_public_model_id.unwrap_or(default_provider_model_id),
            ))
        } else {
            Ok(None)
        }
    }

    async fn resolve_api_key_surface_model(
        &self,
        project_id: &str,
        api_key_id: &str,
        credential_authz_version: i64,
        operation_id: &str,
        api_profiles: &[String],
        requested_public_model_id: &str,
    ) -> Result<Option<ResolvedModelRoute>, ImageGatewayError> {
        self.ensure_api_key_version(project_id, api_key_id, credential_authz_version)
            .await?;
        if api_profiles.is_empty() {
            return Err(ImageGatewayError::internal(
                "model routing API profiles are empty",
            ));
        }
        let rows = sqlx::query_as::<_, ResolvedModelRouteRow>(
            r#"
            SELECT mapping.public_model_id, mapping.api_profile,
                   mapping.provider_id, mapping.operation_id, mapping.command_schema,
                   mapping.provider_model_id,
                   mapping.execution_model_id, mapping.media_kind,
                   mapping.route_id, mapping.route_revision
            FROM gateway_api_key_provider_routes binding
            JOIN gateway_api_keys api_key
              ON api_key.id = binding.api_key_id
             AND api_key.project_id = binding.project_id
             AND api_key.authz_version = $6
             AND api_key.deleted_at IS NULL
            JOIN provider_route_heads head
              ON head.route_id = binding.route_id AND head.state = 'enabled'
            JOIN provider_route_model_mappings mapping
              ON mapping.route_id = binding.route_id
             AND mapping.route_revision = binding.route_revision
             AND mapping.provider_id = binding.provider_id
             AND mapping.operation_id = binding.operation_id
             AND mapping.command_schema = binding.command_schema
            WHERE binding.api_key_id = $1 AND binding.project_id = $2
              AND mapping.operation_id = $3
              AND mapping.api_profile = ANY($4)
              AND mapping.public_model_id = $5
              AND EXISTS (
                SELECT 1
                FROM provider_route_members member
                JOIN provider_execution_profiles profile
                  ON profile.execution_profile_id = member.execution_profile_id
                 AND profile.provider_account_id = member.provider_account_id
                 AND profile.provider_id = member.provider_id
                 AND profile.operation_id = member.operation_id
                 AND profile.command_schema = member.command_schema
                LEFT JOIN provider_account_model_configurations configuration
                  ON configuration.provider_account_id = member.provider_account_id
                 AND configuration.provider_id = member.provider_id
                WHERE member.route_id = mapping.route_id
                  AND member.route_revision = mapping.route_revision
                  AND member.state = 'enabled' AND profile.state = 'enabled'
                  AND (
                    configuration.provider_account_id IS NULL
                    OR configuration.mode = 'automatic'
                    OR EXISTS (
                      SELECT 1 FROM provider_account_model_bindings account_model
                      WHERE account_model.provider_account_id = member.provider_account_id
                        AND account_model.provider_id = member.provider_id
                        AND account_model.model_id = mapping.provider_model_id
                        AND account_model.media_kind = mapping.media_kind
                    )
                  )
              )
            ORDER BY mapping.provider_id, mapping.api_profile
            LIMIT 2
            "#,
        )
        .bind(api_key_id)
        .bind(project_id)
        .bind(operation_id)
        .bind(api_profiles)
        .bind(requested_public_model_id)
        .bind(credential_authz_version)
        .fetch_all(&self.pool)
        .await
        .map_err(store_unavailable)?;
        self.ensure_api_key_version(project_id, api_key_id, credential_authz_version)
            .await?;
        match rows.as_slice() {
            [row] => self
                .ensure_model_allowed(
                    project_id,
                    ResolvedModelRoute {
                        public_model_id: row.public_model_id.clone(),
                        api_profile: row.api_profile.clone(),
                        provider_id: row.provider_id.clone(),
                        operation_id: row.operation_id.clone(),
                        command_schema: row.command_schema.clone(),
                        provider_model_id: row.provider_model_id.clone(),
                        execution_model_id: row.execution_model_id.clone(),
                        media_kind: row.media_kind.clone(),
                        route_id: row.route_id,
                        route_revision: row.route_revision,
                    },
                )
                .await
                .map(Some),
            [] => {
                let bound: bool = sqlx::query_scalar(
                    r#"
                    SELECT EXISTS (
                      SELECT 1
                      FROM gateway_api_key_provider_routes binding
                      JOIN gateway_api_keys api_key
                        ON api_key.id = binding.api_key_id
                       AND api_key.project_id = binding.project_id
                       AND api_key.authz_version = $4
                       AND api_key.deleted_at IS NULL
                      WHERE binding.api_key_id = $1 AND binding.project_id = $2
                        AND binding.operation_id = $3
                    )
                    "#,
                )
                .bind(api_key_id)
                .bind(project_id)
                .bind(operation_id)
                .bind(credential_authz_version)
                .fetch_one(&self.pool)
                .await
                .map_err(store_unavailable)?;
                self.ensure_api_key_version(project_id, api_key_id, credential_authz_version)
                    .await?;
                if bound {
                    Err(ImageGatewayError::model_not_found(
                        requested_public_model_id,
                    ))
                } else {
                    Ok(None)
                }
            }
            _ => Err(ImageGatewayError::service_unavailable(
                "model alias is ambiguous for this API key",
            )),
        }
    }

    async fn list_console_models(
        &self,
        project_id: &str,
        operation_id: &str,
    ) -> Result<Vec<PublicModelRoute>, ImageGatewayError> {
        let rows = sqlx::query_as::<_, ConsoleModelRouteRow>(
            r#"
            WITH effective_binding AS (
              SELECT project.provider_id, project.operation_id, project.command_schema,
                     project.route_id, project.route_revision
              FROM gateway_project_provider_routes project
              WHERE project.project_id = $1
                AND project.operation_id = $2
                AND project.state = 'enabled'
              UNION ALL
              SELECT platform.provider_id, platform.operation_id, platform.command_schema,
                     platform.route_id, platform.route_revision
              FROM gateway_platform_provider_routes platform
              WHERE platform.operation_id = $2
                AND platform.state = 'enabled'
                AND NOT EXISTS (
                  SELECT 1
                  FROM gateway_project_provider_routes project
                  WHERE project.project_id = $1
                    AND project.provider_id = platform.provider_id
                    AND project.operation_id = platform.operation_id
                )
            )
            SELECT mapping.public_model_id, mapping.api_profile,
                   mapping.provider_id, mapping.operation_id, mapping.command_schema,
                   mapping.provider_model_id,
                   mapping.execution_model_id, mapping.media_kind,
                   mapping.route_id, mapping.route_revision, mapping.created_at_ms
            FROM effective_binding binding
            JOIN provider_route_heads head
              ON head.route_id = binding.route_id
             AND head.current_revision = binding.route_revision
             AND head.provider_id = binding.provider_id
             AND head.operation_id = binding.operation_id
             AND head.command_schema = binding.command_schema
            JOIN provider_routes route
              ON route.route_id = head.route_id
             AND route.revision = head.current_revision
             AND route.provider_id = head.provider_id
             AND route.operation_id = head.operation_id
             AND route.command_schema = head.command_schema
            JOIN provider_route_model_mappings mapping
              ON mapping.route_id = route.route_id
             AND mapping.route_revision = route.revision
             AND mapping.provider_id = route.provider_id
             AND mapping.operation_id = route.operation_id
             AND mapping.command_schema = route.command_schema
            WHERE head.state = 'enabled' AND mapping.operation_id = $2
              AND EXISTS (
                SELECT 1
                FROM provider_route_members member
                JOIN provider_execution_profiles profile
                  ON profile.execution_profile_id = member.execution_profile_id
                 AND profile.provider_account_id = member.provider_account_id
                 AND profile.provider_id = member.provider_id
                 AND profile.operation_id = member.operation_id
                 AND profile.command_schema = member.command_schema
                LEFT JOIN provider_account_model_configurations configuration
                  ON configuration.provider_account_id = member.provider_account_id
                 AND configuration.provider_id = member.provider_id
                WHERE member.route_id = mapping.route_id
                  AND member.route_revision = mapping.route_revision
                  AND member.state = 'enabled' AND profile.state = 'enabled'
                  AND (
                    configuration.provider_account_id IS NULL
                    OR configuration.mode = 'automatic'
                    OR EXISTS (
                      SELECT 1 FROM provider_account_model_bindings account_model
                      WHERE account_model.provider_account_id = member.provider_account_id
                        AND account_model.provider_id = member.provider_id
                        AND account_model.model_id = mapping.provider_model_id
                        AND account_model.media_kind = mapping.media_kind
                    )
                  )
              )
            ORDER BY mapping.public_model_id, mapping.api_profile, mapping.provider_id,
                     CASE route.route_kind WHEN 'group' THEN 0 ELSE 1 END,
                     route.route_key
            "#,
        )
        .bind(project_id)
        .bind(operation_id)
        .fetch_all(&self.pool)
        .await
        .map_err(store_unavailable)?;
        let mut models = BTreeMap::new();
        for row in rows {
            let key = (
                row.public_model_id.clone(),
                row.api_profile.clone(),
                row.provider_id.clone(),
                row.operation_id.clone(),
            );
            models.entry(key).or_insert(PublicModelRoute {
                id: row.public_model_id,
                provider_model_id: Some(row.provider_model_id),
                api_profile: row.api_profile,
                provider_id: row.provider_id,
                operation_id: row.operation_id,
                media_kind: row.media_kind,
                created_at_ms: row.created_at_ms,
            });
        }
        Ok(models.into_values().collect())
    }

    async fn resolve_console_model(
        &self,
        project_id: &str,
        provider_id: &str,
        operation_id: &str,
        api_profile: &str,
        requested_public_model_id: Option<&str>,
        default_provider_model_id: &str,
    ) -> Result<Option<ResolvedModelRoute>, ImageGatewayError> {
        let rows = sqlx::query_as::<_, ConsoleModelRouteRow>(
            r#"
            WITH effective_binding AS (
              SELECT project.provider_id, project.operation_id, project.command_schema,
                     project.route_id, project.route_revision
              FROM gateway_project_provider_routes project
              WHERE project.project_id = $1
                AND project.state = 'enabled'
              UNION ALL
              SELECT platform.provider_id, platform.operation_id, platform.command_schema,
                     platform.route_id, platform.route_revision
              FROM gateway_platform_provider_routes platform
              WHERE platform.state = 'enabled'
                AND NOT EXISTS (
                  SELECT 1
                  FROM gateway_project_provider_routes project
                  WHERE project.project_id = $1
                    AND project.provider_id = platform.provider_id
                    AND project.operation_id = platform.operation_id
                )
            )
            SELECT mapping.public_model_id, mapping.api_profile,
                   mapping.provider_id, mapping.operation_id, mapping.command_schema,
                   mapping.provider_model_id,
                   mapping.execution_model_id, mapping.media_kind,
                   mapping.route_id, mapping.route_revision, mapping.created_at_ms
            FROM effective_binding binding
            JOIN provider_route_heads head
              ON head.route_id = binding.route_id
             AND head.current_revision = binding.route_revision
             AND head.provider_id = binding.provider_id
             AND head.operation_id = binding.operation_id
             AND head.command_schema = binding.command_schema
            JOIN provider_routes route
              ON route.route_id = head.route_id
             AND route.revision = head.current_revision
             AND route.provider_id = head.provider_id
             AND route.operation_id = head.operation_id
             AND route.command_schema = head.command_schema
            JOIN provider_route_model_mappings mapping
              ON mapping.route_id = route.route_id
             AND mapping.route_revision = route.revision
             AND mapping.provider_id = route.provider_id
             AND mapping.operation_id = route.operation_id
             AND mapping.command_schema = route.command_schema
            WHERE head.state = 'enabled'
              AND mapping.provider_id = $2
              AND mapping.operation_id = $3
              AND mapping.api_profile = $4
              AND (
                ($5::TEXT IS NOT NULL AND mapping.public_model_id = $5)
                OR ($5::TEXT IS NULL AND mapping.provider_model_id = $6)
              )
              AND EXISTS (
                SELECT 1
                FROM provider_route_members member
                JOIN provider_execution_profiles profile
                  ON profile.execution_profile_id = member.execution_profile_id
                 AND profile.provider_account_id = member.provider_account_id
                 AND profile.provider_id = member.provider_id
                 AND profile.operation_id = member.operation_id
                 AND profile.command_schema = member.command_schema
                LEFT JOIN provider_account_model_configurations configuration
                  ON configuration.provider_account_id = member.provider_account_id
                 AND configuration.provider_id = member.provider_id
                WHERE member.route_id = mapping.route_id
                  AND member.route_revision = mapping.route_revision
                  AND member.state = 'enabled' AND profile.state = 'enabled'
                  AND (
                    configuration.provider_account_id IS NULL
                    OR configuration.mode = 'automatic'
                    OR EXISTS (
                      SELECT 1 FROM provider_account_model_bindings account_model
                      WHERE account_model.provider_account_id = member.provider_account_id
                        AND account_model.provider_id = member.provider_id
                        AND account_model.model_id = mapping.provider_model_id
                        AND account_model.media_kind = mapping.media_kind
                    )
                  )
              )
            ORDER BY CASE route.route_kind WHEN 'group' THEN 0 ELSE 1 END,
                     route.route_key
            "#,
        )
        .bind(project_id)
        .bind(provider_id)
        .bind(operation_id)
        .bind(api_profile)
        .bind(requested_public_model_id)
        .bind(default_provider_model_id)
        .fetch_all(&self.pool)
        .await
        .map_err(store_unavailable)?;
        match select_console_route(rows)? {
            Some(route) => self.ensure_model_allowed(project_id, route).await.map(Some),
            None => Ok(None),
        }
    }

    async fn resolve_console_surface_model(
        &self,
        project_id: &str,
        operation_id: &str,
        api_profiles: &[String],
        requested_public_model_id: &str,
    ) -> Result<Option<ResolvedModelRoute>, ImageGatewayError> {
        if api_profiles.is_empty() {
            return Err(ImageGatewayError::internal(
                "model routing API profiles are empty",
            ));
        }
        let rows = sqlx::query_as::<_, ConsoleModelRouteRow>(
            r#"
            WITH effective_binding AS (
              SELECT project.provider_id, project.operation_id, project.command_schema,
                     project.route_id, project.route_revision
              FROM gateway_project_provider_routes project
              WHERE project.project_id = $1
                AND project.state = 'enabled'
              UNION ALL
              SELECT platform.provider_id, platform.operation_id, platform.command_schema,
                     platform.route_id, platform.route_revision
              FROM gateway_platform_provider_routes platform
              WHERE platform.state = 'enabled'
                AND NOT EXISTS (
                  SELECT 1
                  FROM gateway_project_provider_routes project
                  WHERE project.project_id = $1
                    AND project.provider_id = platform.provider_id
                    AND project.operation_id = platform.operation_id
                )
            )
            SELECT mapping.public_model_id, mapping.api_profile,
                   mapping.provider_id, mapping.operation_id, mapping.command_schema,
                   mapping.provider_model_id,
                   mapping.execution_model_id, mapping.media_kind,
                   mapping.route_id, mapping.route_revision, mapping.created_at_ms
            FROM effective_binding binding
            JOIN provider_route_heads head
              ON head.route_id = binding.route_id
             AND head.current_revision = binding.route_revision
             AND head.provider_id = binding.provider_id
             AND head.operation_id = binding.operation_id
             AND head.command_schema = binding.command_schema
            JOIN provider_routes route
              ON route.route_id = head.route_id
             AND route.revision = head.current_revision
             AND route.provider_id = head.provider_id
             AND route.operation_id = head.operation_id
             AND route.command_schema = head.command_schema
            JOIN provider_route_model_mappings mapping
              ON mapping.route_id = route.route_id
             AND mapping.route_revision = route.revision
             AND mapping.provider_id = route.provider_id
             AND mapping.operation_id = route.operation_id
             AND mapping.command_schema = route.command_schema
            WHERE head.state = 'enabled'
              AND mapping.operation_id = $2
              AND mapping.api_profile = ANY($3)
              AND mapping.public_model_id = $4
              AND EXISTS (
                SELECT 1
                FROM provider_route_members member
                JOIN provider_execution_profiles profile
                  ON profile.execution_profile_id = member.execution_profile_id
                 AND profile.provider_account_id = member.provider_account_id
                 AND profile.provider_id = member.provider_id
                 AND profile.operation_id = member.operation_id
                 AND profile.command_schema = member.command_schema
                LEFT JOIN provider_account_model_configurations configuration
                  ON configuration.provider_account_id = member.provider_account_id
                 AND configuration.provider_id = member.provider_id
                WHERE member.route_id = mapping.route_id
                  AND member.route_revision = mapping.route_revision
                  AND member.state = 'enabled' AND profile.state = 'enabled'
                  AND (
                    configuration.provider_account_id IS NULL
                    OR configuration.mode = 'automatic'
                    OR EXISTS (
                      SELECT 1 FROM provider_account_model_bindings account_model
                      WHERE account_model.provider_account_id = member.provider_account_id
                        AND account_model.provider_id = member.provider_id
                        AND account_model.model_id = mapping.provider_model_id
                        AND account_model.media_kind = mapping.media_kind
                    )
                  )
              )
            ORDER BY CASE route.route_kind WHEN 'group' THEN 0 ELSE 1 END,
                     route.route_key
            "#,
        )
        .bind(project_id)
        .bind(operation_id)
        .bind(api_profiles)
        .bind(requested_public_model_id)
        .fetch_all(&self.pool)
        .await
        .map_err(store_unavailable)?;
        match select_console_route(rows)? {
            Some(route) => self.ensure_model_allowed(project_id, route).await.map(Some),
            None => Ok(None),
        }
    }
}

impl PostgresModelRoutingStore {
    async fn warn_if_api_key_routes_have_no_active_profiles(
        &self,
        project_id: &str,
        api_key_id: &str,
    ) -> Result<(), ImageGatewayError> {
        let broken_route_bindings: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM gateway_api_key_provider_routes binding
            JOIN provider_route_heads head
              ON head.route_id = binding.route_id AND head.state = 'enabled'
            WHERE binding.api_key_id = $1 AND binding.project_id = $2
              AND NOT EXISTS (
                SELECT 1
                FROM provider_route_members member
                JOIN provider_execution_profiles profile
                  ON profile.execution_profile_id = member.execution_profile_id
                 AND profile.provider_account_id = member.provider_account_id
                 AND profile.provider_id = member.provider_id
                 AND profile.operation_id = member.operation_id
                 AND profile.command_schema = member.command_schema
                WHERE member.route_id = binding.route_id
                  AND member.route_revision = binding.route_revision
                  AND member.state = 'enabled'
                  AND profile.state = 'enabled'
              )
            "#,
        )
        .bind(api_key_id)
        .bind(project_id)
        .fetch_one(&self.pool)
        .await
        .map_err(store_unavailable)?;
        if broken_route_bindings > 0 {
            tracing::warn!(
                project_id,
                api_key_id,
                broken_route_bindings,
                "API key model catalog is empty because route bindings have no active execution profile"
            );
        }
        Ok(())
    }
}

fn select_console_route(
    rows: Vec<ConsoleModelRouteRow>,
) -> Result<Option<ResolvedModelRoute>, ImageGatewayError> {
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let ambiguous = rows.iter().skip(1).any(|row| {
        row.public_model_id != first.public_model_id
            || row.api_profile != first.api_profile
            || row.provider_id != first.provider_id
            || row.operation_id != first.operation_id
            || row.command_schema != first.command_schema
            || row.provider_model_id != first.provider_model_id
            || row.execution_model_id != first.execution_model_id
            || row.media_kind != first.media_kind
    });
    if ambiguous {
        return Err(ImageGatewayError::service_unavailable(
            "model alias is ambiguous across console routes",
        ));
    }
    Ok(Some(ResolvedModelRoute {
        public_model_id: first.public_model_id.clone(),
        api_profile: first.api_profile.clone(),
        provider_id: first.provider_id.clone(),
        operation_id: first.operation_id.clone(),
        command_schema: first.command_schema.clone(),
        provider_model_id: first.provider_model_id.clone(),
        execution_model_id: first.execution_model_id.clone(),
        media_kind: first.media_kind.clone(),
        route_id: first.route_id,
        route_revision: first.route_revision,
    }))
}

fn store_unavailable(_: sqlx::Error) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("model routing is unavailable")
}
