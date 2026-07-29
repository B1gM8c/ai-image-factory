CREATE TABLE api_profile_pricing_aliases (
    api_profile TEXT PRIMARY KEY,
    pricing_api_profile TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    CHECK (api_profile <> ''),
    CHECK (pricing_api_profile <> ''),
    CHECK (api_profile <> pricing_api_profile)
);

INSERT INTO api_profile_pricing_aliases
  (api_profile, pricing_api_profile, created_at_ms)
VALUES
  ('volcengine-ark-images-v3', 'dreamina-cli-images-v1', 0),
  ('volcengine-ark-content-generation-v3', 'dreamina-cli-videos-v1', 0);
