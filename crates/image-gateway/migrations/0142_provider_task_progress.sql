-- Progress belongs to one immutable poll observation. Legacy observations stay
-- NULL, and the task's existing state_observation_id selects the latest report.
ALTER TABLE provider_task_observations
    ADD COLUMN progress_json JSONB,
    ADD CONSTRAINT provider_task_progress_payload_check CHECK (
        progress_json IS NULL OR (
            source = 'poll' AND observed_state = 'provider_waiting'
            AND jsonb_typeof(progress_json) = 'object'
            AND octet_length(progress_json::TEXT) <= 1024
            AND progress_json - ARRAY[
                'provider_status', 'queue_position', 'queue_total',
                'estimated_remaining_seconds'
            ] = '{}'::JSONB
            AND progress_json ? 'provider_status'
            AND jsonb_typeof(progress_json -> 'provider_status') = 'string'
            AND progress_json ->> 'provider_status' ~ '^[A-Za-z0-9_-]{1,64}$'
            AND CASE
                WHEN progress_json -> 'queue_position' IS NULL
                    OR progress_json -> 'queue_position' = 'null'::JSONB THEN TRUE
                WHEN jsonb_typeof(progress_json -> 'queue_position') = 'number' THEN
                    (progress_json ->> 'queue_position')::NUMERIC BETWEEN 0 AND 4294967295
                    AND trunc((progress_json ->> 'queue_position')::NUMERIC)
                        = (progress_json ->> 'queue_position')::NUMERIC
                ELSE FALSE
            END
            AND CASE
                WHEN progress_json -> 'queue_total' IS NULL
                    OR progress_json -> 'queue_total' = 'null'::JSONB THEN TRUE
                WHEN jsonb_typeof(progress_json -> 'queue_total') = 'number' THEN
                    (progress_json ->> 'queue_total')::NUMERIC BETWEEN 0 AND 4294967295
                    AND trunc((progress_json ->> 'queue_total')::NUMERIC)
                        = (progress_json ->> 'queue_total')::NUMERIC
                ELSE FALSE
            END
            AND CASE
                WHEN progress_json -> 'estimated_remaining_seconds' IS NULL
                    OR progress_json -> 'estimated_remaining_seconds' = 'null'::JSONB THEN TRUE
                WHEN jsonb_typeof(progress_json -> 'estimated_remaining_seconds') = 'number' THEN
                    (progress_json ->> 'estimated_remaining_seconds')::NUMERIC BETWEEN 0 AND 2592000
                    AND trunc((progress_json ->> 'estimated_remaining_seconds')::NUMERIC)
                        = (progress_json ->> 'estimated_remaining_seconds')::NUMERIC
                ELSE FALSE
            END
            AND CASE
                WHEN jsonb_typeof(progress_json -> 'queue_position') = 'number'
                    AND jsonb_typeof(progress_json -> 'queue_total') = 'number' THEN
                    (progress_json ->> 'queue_position')::NUMERIC <= (progress_json ->> 'queue_total')::NUMERIC
                ELSE TRUE
            END
        )
    );
