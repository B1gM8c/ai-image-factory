ALTER TABLE executor_artifact_authorities
    DROP CONSTRAINT executor_artifact_authorities_media_type_check,
    ADD CONSTRAINT executor_artifact_authorities_media_type_check CHECK (
        media_type IN ('image/png', 'image/jpeg', 'image/webp', 'video/mp4')
    );

ALTER TABLE provider_task_observations
    DROP CONSTRAINT provider_task_observations_artifact_manifest_check,
    ADD CONSTRAINT provider_task_observations_artifact_manifest_check CHECK (
        (observed_state = 'artifact_ready'
            AND result_manifest_id IS NOT NULL
            AND artifact_sha256_hex ~ '^[0-9a-f]{64}$'
            AND artifact_byte_size BETWEEN 1 AND 268435456
            AND artifact_media_type IN ('image/png', 'image/jpeg', 'image/webp', 'video/mp4'))
        OR
        (observed_state <> 'artifact_ready'
            AND result_manifest_id IS NULL
            AND artifact_sha256_hex IS NULL
            AND artifact_byte_size IS NULL
            AND artifact_media_type IS NULL)
    );

ALTER TABLE artifacts
    DROP CONSTRAINT artifacts_media_type_check,
    ADD CONSTRAINT artifacts_media_type_check CHECK (
        media_type IN ('image/png', 'image/jpeg', 'image/webp', 'video/mp4')
    );
