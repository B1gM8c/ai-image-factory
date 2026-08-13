-- Standard-service API keys historically had no provider-route rows, while
-- runtime authorization deliberately resolves API-key traffic only through
-- gateway_api_key_provider_routes. Materialize the same effective project /
-- platform routes used by the console. Keys with any explicit binding remain
-- pinned and are not widened.
WITH standard_keys AS (
    SELECT api_key.id AS api_key_id,
           api_key.service_account_id,
           api_key.project_id,
           api_key.tenant_id
    FROM gateway_api_keys api_key
    JOIN gateway_service_accounts service_account
      ON service_account.id = api_key.service_account_id
     AND service_account.project_id = api_key.project_id
     AND service_account.tenant_id = api_key.tenant_id
    JOIN gateway_projects project
      ON project.id = api_key.project_id
     AND project.tenant_id = api_key.tenant_id
    WHERE api_key.deleted_at IS NULL
      AND service_account.deleted_at IS NULL
      AND project.archived_at IS NULL
      AND NOT EXISTS (
          SELECT 1
          FROM gateway_api_key_provider_routes existing
          WHERE existing.api_key_id = api_key.id
      )
),
effective_routes AS (
    SELECT standard.api_key_id,
           standard.service_account_id,
           standard.project_id,
           standard.tenant_id,
           project.provider_id,
           project.operation_id,
           project.command_schema,
           project.route_id,
           project.route_revision
    FROM standard_keys standard
    JOIN gateway_project_provider_routes project
      ON project.project_id = standard.project_id
     AND project.state = 'enabled'
    JOIN provider_route_heads head
      ON head.route_id = project.route_id
     AND head.current_revision = project.route_revision
     AND head.provider_id = project.provider_id
     AND head.operation_id = project.operation_id
     AND head.command_schema = project.command_schema
     AND head.state = 'enabled'
    UNION ALL
    SELECT standard.api_key_id,
           standard.service_account_id,
           standard.project_id,
           standard.tenant_id,
           platform.provider_id,
           platform.operation_id,
           platform.command_schema,
           platform.route_id,
           platform.route_revision
    FROM standard_keys standard
    JOIN gateway_platform_provider_routes platform
      ON platform.state = 'enabled'
    JOIN provider_route_heads head
      ON head.route_id = platform.route_id
     AND head.current_revision = platform.route_revision
     AND head.provider_id = platform.provider_id
     AND head.operation_id = platform.operation_id
     AND head.command_schema = platform.command_schema
     AND head.state = 'enabled'
    WHERE NOT EXISTS (
        SELECT 1
        FROM gateway_project_provider_routes project
        WHERE project.project_id = standard.project_id
          AND project.provider_id = platform.provider_id
          AND project.operation_id = platform.operation_id
    )
)
INSERT INTO gateway_api_key_provider_routes (
    api_key_id,
    service_account_id,
    project_id,
    tenant_id,
    provider_id,
    operation_id,
    command_schema,
    route_id,
    route_revision,
    bound_at_ms
)
SELECT api_key_id,
       service_account_id,
       project_id,
       tenant_id,
       provider_id,
       operation_id,
       command_schema,
       route_id,
       route_revision,
       floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
FROM effective_routes;
