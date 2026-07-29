CREATE TABLE pricing_source_snapshots (
    snapshot_id UUID PRIMARY KEY,
    catalog_key TEXT NOT NULL CHECK (
        char_length(catalog_key) BETWEEN 1 AND 128
        AND catalog_key ~ '^[A-Za-z0-9_.:-]+$'
    ),
    source_provider_id TEXT NOT NULL CHECK (
        char_length(source_provider_id) BETWEEN 1 AND 128
        AND source_provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    source_url TEXT NOT NULL CHECK (
        char_length(source_url) BETWEEN 1 AND 2048
    ),
    source_checked_at_ms BIGINT NOT NULL CHECK (source_checked_at_ms > 0),
    source_revision TEXT CHECK (
        source_revision IS NULL OR char_length(source_revision) <= 255
    ),
    parser_version TEXT NOT NULL CHECK (
        char_length(parser_version) BETWEEN 1 AND 64
        AND parser_version ~ '^[A-Za-z0-9_.:-]+$'
    ),
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
    state TEXT NOT NULL CHECK (
        state IN ('observed', 'partially_applied', 'applied', 'rejected')
    ),
    item_count INTEGER NOT NULL CHECK (item_count > 0),
    normalized_payload JSONB NOT NULL CHECK (
        jsonb_typeof(normalized_payload) = 'object'
    ),
    created_by_user_id UUID NOT NULL
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (catalog_key, content_sha256),
    CHECK (updated_at_ms >= created_at_ms)
);

CREATE INDEX pricing_source_snapshots_provider_created_idx
    ON pricing_source_snapshots (
        source_provider_id, created_at_ms DESC, snapshot_id DESC
    );

CREATE TABLE pricing_source_snapshot_applications (
    snapshot_id UUID NOT NULL
        REFERENCES pricing_source_snapshots(snapshot_id) ON DELETE RESTRICT,
    item_key TEXT NOT NULL CHECK (
        char_length(item_key) BETWEEN 1 AND 128
        AND item_key ~ '^[A-Za-z0-9_.:-]+$'
    ),
    price_book_id UUID NOT NULL
        REFERENCES price_books(price_book_id) ON DELETE RESTRICT,
    price_book_version_id UUID NOT NULL,
    action TEXT NOT NULL CHECK (
        action IN ('created_draft', 'linked_draft', 'linked_active')
    ),
    applied_by_user_id UUID NOT NULL
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    applied_at_ms BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id, item_key),
    FOREIGN KEY (price_book_version_id, price_book_id)
        REFERENCES price_book_versions(price_book_version_id, price_book_id)
        ON DELETE RESTRICT
);

CREATE INDEX pricing_source_snapshot_applications_version_idx
    ON pricing_source_snapshot_applications(price_book_version_id, snapshot_id);
