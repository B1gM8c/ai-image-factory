CREATE TABLE price_book_version_rollbacks (
    rollback_version_id UUID PRIMARY KEY
        REFERENCES price_book_versions(price_book_version_id) ON DELETE RESTRICT,
    source_version_id UUID NOT NULL
        REFERENCES price_book_versions(price_book_version_id) ON DELETE RESTRICT,
    created_by_user_id UUID NOT NULL
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    created_by_session_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,
    CHECK (rollback_version_id <> source_version_id)
);

CREATE INDEX price_book_version_rollbacks_source_idx
    ON price_book_version_rollbacks(source_version_id, created_at_ms DESC);

CREATE OR REPLACE FUNCTION preserve_price_book_version_rollback()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'price book rollback lineage is immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER preserve_price_book_version_rollback
BEFORE UPDATE OR DELETE ON price_book_version_rollbacks
FOR EACH ROW EXECUTE FUNCTION preserve_price_book_version_rollback();
