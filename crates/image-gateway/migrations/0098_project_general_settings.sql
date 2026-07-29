ALTER TABLE gateway_projects
  ADD COLUMN user_api_keys_disabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN settings_version BIGINT NOT NULL DEFAULT 1;

ALTER TABLE gateway_projects
  ADD CONSTRAINT gateway_projects_settings_version_check
  CHECK (settings_version > 0);

COMMENT ON COLUMN gateway_projects.user_api_keys_disabled IS
  'When true, user-owned API keys cannot be created or authenticated; service-account keys remain active.';

COMMENT ON COLUMN gateway_projects.settings_version IS
  'Optimistic concurrency version for project general settings.';
