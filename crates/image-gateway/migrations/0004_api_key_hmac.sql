ALTER TABLE gateway_api_keys
    ADD COLUMN hash_algorithm TEXT NOT NULL DEFAULT 'sha256',
    ADD COLUMN pepper_version INTEGER;

ALTER TABLE gateway_api_keys
    ADD CONSTRAINT gateway_api_keys_hash_metadata_check CHECK (
        char_length(key_hash) = 64
        AND (
            (hash_algorithm = 'sha256' AND pepper_version IS NULL)
            OR
            (hash_algorithm = 'hmac-sha256-v1' AND pepper_version BETWEEN 1 AND 65535)
        )
    );
