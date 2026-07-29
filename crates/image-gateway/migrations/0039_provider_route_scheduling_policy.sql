ALTER TABLE provider_routes
    ADD COLUMN quota_freshness_ms BIGINT NOT NULL DEFAULT 900000 CHECK (
        quota_freshness_ms BETWEEN 60000 AND 86400000
    ),
    ADD COLUMN unknown_quota_policy TEXT NOT NULL DEFAULT 'allow' CHECK (
        unknown_quota_policy IN ('allow', 'block')
    );

ALTER TABLE provider_route_members
    ADD COLUMN minimum_remaining_percent SMALLINT NOT NULL DEFAULT 0 CHECK (
        minimum_remaining_percent BETWEEN 0 AND 100
    );

COMMENT ON COLUMN provider_routes.quota_freshness_ms IS
    'Maximum age of quota evidence used by route admission.';
COMMENT ON COLUMN provider_routes.unknown_quota_policy IS
    'Whether a member without fresh quota evidence remains eligible.';
COMMENT ON COLUMN provider_route_members.minimum_remaining_percent IS
    'Member is protected when any active window falls below this remaining percentage.';
