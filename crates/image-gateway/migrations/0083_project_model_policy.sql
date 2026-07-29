CREATE TABLE project_model_policies (
    project_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    control_version BIGINT NOT NULL DEFAULT 1 CHECK (control_version > 0),
    created_by_user_id UUID NOT NULL
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    updated_by_user_id UUID NOT NULL
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (project_id, organization_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT,
    CHECK (updated_at_ms >= created_at_ms)
);

CREATE TABLE project_model_access_entries (
    project_id TEXT NOT NULL
        REFERENCES project_model_policies(project_id) ON DELETE CASCADE,
    operation_id TEXT NOT NULL CHECK (
        char_length(operation_id) BETWEEN 1 AND 128
        AND operation_id !~ '[[:cntrl:]]'
    ),
    api_profile TEXT NOT NULL CHECK (
        char_length(api_profile) BETWEEN 1 AND 128
        AND api_profile !~ '[[:cntrl:]]'
    ),
    public_model_id TEXT NOT NULL CHECK (
        char_length(public_model_id) BETWEEN 1 AND 255
        AND public_model_id ~ '^[A-Za-z0-9_.:-]+$'
    ),
    media_kind TEXT NOT NULL CHECK (media_kind IN ('image', 'video')),
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, operation_id, api_profile, public_model_id)
);

CREATE TABLE platform_model_limit_members (
    operation_id TEXT NOT NULL CHECK (
        char_length(operation_id) BETWEEN 1 AND 128
        AND operation_id !~ '[[:cntrl:]]'
    ),
    api_profile TEXT NOT NULL CHECK (
        char_length(api_profile) BETWEEN 1 AND 128
        AND api_profile !~ '[[:cntrl:]]'
    ),
    public_model_id TEXT NOT NULL CHECK (
        char_length(public_model_id) BETWEEN 1 AND 255
        AND public_model_id ~ '^[A-Za-z0-9_.:-]+$'
    ),
    media_kind TEXT NOT NULL CHECK (media_kind IN ('image', 'video')),
    bucket_key TEXT NOT NULL CHECK (
        char_length(bucket_key) BETWEEN 1 AND 384
        AND bucket_key !~ '[[:cntrl:]]'
    ),
    bucket_display_name TEXT NOT NULL CHECK (
        char_length(bucket_display_name) BETWEEN 1 AND 255
        AND bucket_display_name !~ '[[:cntrl:]]'
    ),
    unit_kind TEXT NOT NULL CHECK (unit_kind IN ('image', 'video_second')),
    request_ceiling_per_minute INTEGER
        CHECK (request_ceiling_per_minute > 0),
    unit_ceiling_per_minute INTEGER
        CHECK (unit_ceiling_per_minute > 0),
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (operation_id, api_profile, public_model_id),
    CHECK (
        (media_kind = 'image' AND unit_kind = 'image')
        OR (media_kind = 'video' AND unit_kind = 'video_second')
    )
);

CREATE INDEX platform_model_limit_members_bucket_idx
    ON platform_model_limit_members(bucket_key, operation_id, api_profile, public_model_id);

CREATE TABLE project_model_rate_limits (
    project_id TEXT NOT NULL
        REFERENCES project_model_policies(project_id) ON DELETE CASCADE,
    bucket_key TEXT NOT NULL CHECK (
        char_length(bucket_key) BETWEEN 1 AND 384
        AND bucket_key !~ '[[:cntrl:]]'
    ),
    unit_kind TEXT NOT NULL CHECK (unit_kind IN ('image', 'video_second')),
    request_limit_per_minute INTEGER
        CHECK (request_limit_per_minute > 0),
    unit_limit_per_minute INTEGER
        CHECK (unit_limit_per_minute > 0),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, bucket_key),
    CHECK (
        request_limit_per_minute IS NOT NULL
        OR unit_limit_per_minute IS NOT NULL
    ),
    CHECK (updated_at_ms >= created_at_ms)
);

CREATE TABLE project_model_rate_states (
    project_id TEXT NOT NULL,
    bucket_key TEXT NOT NULL,
    request_tokens_microunits BIGINT
        CHECK (request_tokens_microunits >= 0),
    unit_tokens_microunits BIGINT
        CHECK (unit_tokens_microunits >= 0),
    last_refill_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, bucket_key),
    FOREIGN KEY (project_id, bucket_key)
        REFERENCES project_model_rate_limits(project_id, bucket_key) ON DELETE CASCADE,
    CHECK (updated_at_ms >= last_refill_at_ms)
);

CREATE TABLE project_model_rate_admissions (
    project_id TEXT NOT NULL,
    bucket_key TEXT NOT NULL,
    admission_session_id UUID NOT NULL,
    request_units INTEGER NOT NULL DEFAULT 1 CHECK (request_units = 1),
    unit_count INTEGER NOT NULL CHECK (unit_count > 0),
    admitted_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, admission_session_id),
    FOREIGN KEY (project_id, bucket_key)
        REFERENCES project_model_rate_limits(project_id, bucket_key) ON DELETE CASCADE
);

CREATE INDEX project_model_rate_admissions_bucket_idx
    ON project_model_rate_admissions(
        project_id,
        bucket_key,
        admitted_at_ms DESC,
        admission_session_id
    );

INSERT INTO platform_model_limit_members(
    operation_id, api_profile, public_model_id, media_kind,
    bucket_key, bucket_display_name, unit_kind, created_at_ms
)
VALUES
    ('images.generations', 'openai-images-v1', 'gpt-image-2', 'image',
     'openai:gpt-image-2', 'GPT Image 2', 'image', 0),
    ('images.generations', 'openai-images-v1', 'gpt-image-2-2026-04-21', 'image',
     'openai:gpt-image-2', 'GPT Image 2', 'image', 0),
    ('images.generations', 'dreamina-cli-images-v1', '5.0', 'image',
     'dreamina:seedream-5-lite', 'Seedream 5 Lite', 'image', 0),
    ('images.generations', 'volcengine-ark-images-v3', 'doubao-seedream-5-0-lite', 'image',
     'dreamina:seedream-5-lite', 'Seedream 5 Lite', 'image', 0),
    ('images.generations', 'dreamina-cli-images-v1', '5.0Pro', 'image',
     'dreamina:seedream-5-pro', 'Seedream 5 Pro', 'image', 0),
    ('images.generations', 'volcengine-ark-images-v3', 'doubao-seedream-5-0-260128', 'image',
     'dreamina:seedream-5-pro', 'Seedream 5 Pro', 'image', 0),
    ('videos.generations', 'dreamina-cli-videos-v1', 'seedance2.0', 'video',
     'dreamina:seedance-2', 'Seedance 2', 'video_second', 0),
    ('videos.generations', 'volcengine-ark-content-generation-v3',
     'doubao-seedance-2-0-260128', 'video',
     'dreamina:seedance-2', 'Seedance 2', 'video_second', 0),
    ('videos.generations', 'dreamina-cli-videos-v1', 'seedance2.0fast', 'video',
     'dreamina:seedance-2-fast', 'Seedance 2 Fast', 'video_second', 0),
    ('videos.generations', 'volcengine-ark-content-generation-v3',
     'doubao-seedance-2-0-fast-260128', 'video',
     'dreamina:seedance-2-fast', 'Seedance 2 Fast', 'video_second', 0),
    ('videos.generations', 'dreamina-cli-videos-v1', 'seedance2.0mini', 'video',
     'dreamina:seedance-2-mini', 'Seedance 2 Mini', 'video_second', 0),
    ('videos.generations', 'volcengine-ark-content-generation-v3',
     'doubao-seedance-2-0-mini-260128', 'video',
     'dreamina:seedance-2-mini', 'Seedance 2 Mini', 'video_second', 0)
ON CONFLICT (operation_id, api_profile, public_model_id) DO NOTHING;

COMMENT ON TABLE project_model_policies IS
    'Project allow-list authority. Absence means all currently routable models are allowed; once configured, only explicit entries are allowed.';

COMMENT ON TABLE platform_model_limit_members IS
    'Stable public-model to shared rate-limit bucket mapping. Protocol aliases for one native model share one bucket.';

COMMENT ON TABLE project_model_rate_states IS
    'Transactional token-bucket state in integer microunits. Capacity equals one minute of the configured project override.';

COMMENT ON TABLE project_model_rate_admissions IS
    'Accepted project/model rate admissions, idempotent by admission session. Provider failure does not refund rate tokens.';
