ALTER TABLE quota_reservations
    ADD COLUMN admission_session_id UUID UNIQUE
        REFERENCES admission_sessions(session_id) ON DELETE RESTRICT;

ALTER TABLE admission_sessions
    ADD COLUMN input_cleanup_state TEXT NOT NULL DEFAULT 'pending'
        CHECK (input_cleanup_state IN ('pending', 'leased', 'complete')),
    ADD COLUMN input_cleanup_owner TEXT,
    ADD COLUMN input_cleanup_lease_expires_at_ms BIGINT,
    ADD COLUMN input_cleanup_completed_at_ms BIGINT,
    ADD CONSTRAINT admission_input_cleanup_state_consistent CHECK (
        (input_cleanup_state = 'pending'
            AND input_cleanup_owner IS NULL
            AND input_cleanup_lease_expires_at_ms IS NULL
            AND input_cleanup_completed_at_ms IS NULL)
        OR
        (input_cleanup_state = 'leased'
            AND input_cleanup_owner IS NOT NULL
            AND input_cleanup_owner <> ''
            AND input_cleanup_lease_expires_at_ms IS NOT NULL
            AND input_cleanup_completed_at_ms IS NULL)
        OR
        (input_cleanup_state = 'complete'
            AND input_cleanup_owner IS NULL
            AND input_cleanup_lease_expires_at_ms IS NULL
            AND input_cleanup_completed_at_ms IS NOT NULL)
    );

CREATE INDEX admission_input_cleanup_pending_idx
    ON admission_sessions (state, updated_at_ms, session_id)
    WHERE operation = 'edit' AND input_cleanup_state = 'pending';

CREATE INDEX admission_input_cleanup_lease_idx
    ON admission_sessions (input_cleanup_lease_expires_at_ms, session_id)
    WHERE operation = 'edit' AND input_cleanup_state = 'leased';

CREATE TABLE job_input_manifests (
    job_id UUID PRIMARY KEY REFERENCES jobs(job_id) ON DELETE RESTRICT,
    admission_session_id UUID NOT NULL UNIQUE
        REFERENCES admission_sessions(session_id) ON DELETE RESTRICT,
    manifest_schema TEXT NOT NULL,
    manifest_hash TEXT NOT NULL CHECK (char_length(manifest_hash) = 64),
    input_count SMALLINT NOT NULL CHECK (input_count BETWEEN 1 AND 17),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (job_id, admission_session_id)
);

CREATE TABLE job_input_objects (
    input_id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES job_input_manifests(job_id) ON DELETE RESTRICT,
    admission_session_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('image', 'mask')),
    input_index SMALLINT NOT NULL CHECK (input_index BETWEEN 0 AND 15),
    media_type TEXT NOT NULL CHECK (media_type IN ('image/png', 'image/jpeg', 'image/webp')),
    storage_backend TEXT NOT NULL,
    object_key TEXT NOT NULL CHECK (object_key <> ''),
    sha256_hex TEXT NOT NULL CHECK (char_length(sha256_hex) = 64),
    byte_size BIGINT NOT NULL CHECK (byte_size > 0),
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (job_id, admission_session_id)
        REFERENCES job_input_manifests(job_id, admission_session_id) ON DELETE RESTRICT,
    UNIQUE (job_id, role, input_index),
    UNIQUE (storage_backend, object_key),
    CHECK (role <> 'mask' OR (input_index = 0 AND media_type = 'image/png'))
);

CREATE INDEX job_input_objects_session_idx
    ON job_input_objects (admission_session_id, role, input_index);

ALTER TABLE job_response_projections
    ADD COLUMN operation TEXT NOT NULL DEFAULT 'generation'
        CHECK (operation IN ('generation', 'edit'));

UPDATE job_response_projections rp
SET operation = j.operation
FROM jobs j
WHERE j.job_id = rp.job_id;

ALTER TABLE job_response_projections
    ALTER COLUMN operation DROP DEFAULT;
