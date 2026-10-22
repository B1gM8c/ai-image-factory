-- Storage descriptors only; provider admission must validate actual media bytes.
ALTER TABLE job_input_manifests
    DROP CONSTRAINT job_input_manifests_input_count_check,
    ADD CONSTRAINT job_input_manifests_input_count_check
        CHECK (input_count BETWEEN 1 AND 50);

ALTER TABLE job_input_objects
    DROP CONSTRAINT job_input_objects_role_check,
    DROP CONSTRAINT job_input_objects_input_index_check,
    DROP CONSTRAINT job_input_objects_media_type_check,
    ADD CONSTRAINT job_input_objects_role_check
        CHECK (role IN ('image', 'mask', 'video', 'audio')),
    ADD CONSTRAINT job_input_objects_input_index_check
        CHECK (input_index BETWEEN 0 AND 49),
    ADD CONSTRAINT job_input_objects_media_type_check
        CHECK (media_type IN (
            'image/png', 'image/jpeg', 'image/webp',
            'video/mp4', 'audio/mpeg', 'audio/wav'
        )),
    ADD CONSTRAINT job_input_objects_role_media_type_check CHECK (
        (role = 'image' AND media_type IN ('image/png', 'image/jpeg', 'image/webp'))
        OR (role = 'mask' AND input_index = 0 AND media_type = 'image/png')
        OR (role = 'video' AND media_type = 'video/mp4')
        OR (role = 'audio' AND media_type IN ('audio/mpeg', 'audio/wav'))
    );

DROP INDEX admission_input_cleanup_pending_idx;
CREATE INDEX admission_input_cleanup_pending_idx
    ON admission_sessions (state, updated_at_ms, session_id)
    WHERE operation IN ('generation', 'edit', 'video_generation')
        AND input_cleanup_state = 'pending';

DROP INDEX admission_input_cleanup_lease_idx;
CREATE INDEX admission_input_cleanup_lease_idx
    ON admission_sessions (input_cleanup_lease_expires_at_ms, session_id)
    WHERE operation IN ('generation', 'edit', 'video_generation')
        AND input_cleanup_state = 'leased';
