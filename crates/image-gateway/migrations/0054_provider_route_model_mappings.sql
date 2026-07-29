CREATE TABLE provider_route_model_mappings (
    route_id UUID NOT NULL,
    route_revision BIGINT NOT NULL,
    provider_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    command_schema TEXT NOT NULL,
    api_profile TEXT NOT NULL CHECK (
        char_length(api_profile) BETWEEN 1 AND 128
        AND api_profile ~ '^[A-Za-z0-9_.-]+$'
    ),
    public_model_id TEXT NOT NULL CHECK (
        char_length(public_model_id) BETWEEN 1 AND 255
        AND public_model_id ~ '^[A-Za-z0-9_.:-]+$'
    ),
    provider_model_id TEXT NOT NULL,
    execution_model_id TEXT NOT NULL CHECK (
        char_length(execution_model_id) BETWEEN 1 AND 255
        AND execution_model_id !~ '[[:cntrl:]]'
    ),
    media_kind TEXT NOT NULL CHECK (media_kind IN ('image', 'video')),
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (route_id, route_revision, api_profile, public_model_id),
    UNIQUE (route_id, route_revision, api_profile, execution_model_id),
    FOREIGN KEY (
        route_id, route_revision, provider_id, operation_id, command_schema
    ) REFERENCES provider_routes(
        route_id, revision, provider_id, operation_id, command_schema
    ) ON DELETE RESTRICT,
    FOREIGN KEY (provider_id, provider_model_id, media_kind)
        REFERENCES provider_models(provider_id, model_id, media_kind)
        ON DELETE RESTRICT
);

CREATE INDEX provider_route_model_mappings_resolve_idx
    ON provider_route_model_mappings
       (api_profile, public_model_id, provider_id, operation_id, route_id, route_revision);

CREATE INDEX provider_route_model_mappings_target_idx
    ON provider_route_model_mappings
       (route_id, route_revision, provider_model_id, media_kind);

CREATE FUNCTION reject_provider_route_model_mapping_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider route revision model mappings are immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_route_model_mappings_immutable
BEFORE UPDATE OR DELETE ON provider_route_model_mappings
FOR EACH ROW EXECUTE FUNCTION reject_provider_route_model_mapping_mutation();

-- Existing route revisions receive the least-surprising external identifier.
-- Dreamina routes use the current Volcengine Ark names where the adapter has an
-- official equivalent; other adapters retain their native model identifiers.
INSERT INTO provider_route_model_mappings
  (route_id, route_revision, provider_id, operation_id, command_schema,
   api_profile, public_model_id, provider_model_id, execution_model_id,
   media_kind, created_at_ms)
SELECT route.route_id,
       route.revision,
       route.provider_id,
       route.operation_id,
       route.command_schema,
       profile.api_profile,
       profile.public_model_id,
       model.model_id,
       model.execution_model_id,
       model.media_kind,
       route.created_at_ms
FROM provider_routes route
JOIN provider_models model
  ON model.provider_id = route.provider_id
 AND route.operation_id = ANY(model.operation_ids)
 AND model.adapter_state = 'supported'
 AND model.lifecycle_state = 'enabled'
CROSS JOIN LATERAL (
  SELECT CASE
           WHEN route.provider_id = 'openai-codex' THEN 'openai-images-v1'
           WHEN route.provider_id = 'grok-cli' AND route.operation_id = 'images.generations'
             THEN 'xai-images-v1'
           WHEN route.provider_id = 'grok-cli' AND route.operation_id = 'videos.generations'
             THEN 'xai-videos-v1'
           WHEN route.provider_id = 'dreamina-cli' AND route.operation_id = 'images.generations'
             THEN 'dreamina-cli-images-v1'
           WHEN route.provider_id = 'dreamina-cli' AND route.operation_id = 'videos.generations'
             THEN 'dreamina-cli-videos-v1'
         END AS api_profile,
         model.model_id AS public_model_id
  UNION ALL
  SELECT CASE
           WHEN route.operation_id = 'images.generations'
             THEN 'volcengine-ark-images-v3'
           ELSE 'volcengine-ark-content-generation-v3'
         END,
         CASE
           WHEN model.model_id = '5.0' THEN 'doubao-seedream-5-0-lite'
           WHEN model.model_id = '5.0Pro' THEN 'doubao-seedream-5-0-260128'
           WHEN model.model_id = 'seedance2.0' THEN 'doubao-seedance-2-0-260128'
           WHEN model.model_id = 'seedance2.0fast' THEN 'doubao-seedance-2-0-fast-260128'
           WHEN model.model_id = 'seedance2.0mini' THEN 'doubao-seedance-2-0-mini-260128'
         END
  WHERE route.provider_id = 'dreamina-cli'
    AND model.model_id IN ('5.0', '5.0Pro', 'seedance2.0', 'seedance2.0fast', 'seedance2.0mini')
) profile
WHERE profile.api_profile IS NOT NULL AND profile.public_model_id IS NOT NULL
ON CONFLICT (route_id, route_revision, api_profile, public_model_id) DO NOTHING;
