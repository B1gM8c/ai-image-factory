-- Provider-account reconciliation can create enabled route heads after the
-- original platform-binding migration has already run. Backfill only the
-- provider/operation pairs that have exactly one enabled current route; an
-- existing operator-selected platform binding always wins.
WITH candidates AS (
    SELECT
        route.provider_id,
        route.operation_id,
        route.command_schema,
        route.route_id,
        route.revision,
        route.created_at_ms,
        count(*) OVER (
            PARTITION BY route.provider_id, route.operation_id
        ) AS candidate_count
    FROM provider_routes route
    JOIN provider_route_heads head
      ON head.route_id = route.route_id
     AND head.current_revision = route.revision
     AND head.provider_id = route.provider_id
     AND head.operation_id = route.operation_id
     AND head.command_schema = route.command_schema
    WHERE head.state = 'enabled'
      AND route.state = 'enabled'
      AND EXISTS (
          SELECT 1
          FROM provider_route_model_mappings mapping
          WHERE mapping.route_id = route.route_id
            AND mapping.route_revision = route.revision
            AND mapping.provider_id = route.provider_id
            AND mapping.operation_id = route.operation_id
            AND mapping.command_schema = route.command_schema
      )
)
INSERT INTO gateway_platform_provider_routes (
    provider_id,
    operation_id,
    command_schema,
    route_id,
    route_revision,
    state,
    created_at_ms,
    updated_at_ms
)
SELECT
    provider_id,
    operation_id,
    command_schema,
    route_id,
    revision,
    'enabled',
    created_at_ms,
    created_at_ms
FROM candidates
WHERE candidate_count = 1
ON CONFLICT (provider_id, operation_id) DO NOTHING;
