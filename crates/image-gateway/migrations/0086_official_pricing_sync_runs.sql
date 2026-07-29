CREATE TABLE pricing_source_sync_runs (
    sync_run_id UUID PRIMARY KEY,
    catalog_key TEXT NOT NULL CHECK (
        char_length(catalog_key) BETWEEN 1 AND 128
        AND catalog_key ~ '^[A-Za-z0-9_.:-]+$'
    ),
    source_provider_id TEXT NOT NULL CHECK (
        char_length(source_provider_id) BETWEEN 1 AND 128
        AND source_provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    source_url TEXT NOT NULL CHECK (
        char_length(source_url) BETWEEN 1 AND 2048
    ),
    retrieval_method TEXT NOT NULL CHECK (
        retrieval_method IN (
            'curated_manifest', 'official_api', 'official_document'
        )
    ),
    parser_version TEXT NOT NULL CHECK (
        char_length(parser_version) BETWEEN 1 AND 64
        AND parser_version ~ '^[A-Za-z0-9_.:-]+$'
    ),
    source_checked_at_ms BIGINT NOT NULL CHECK (source_checked_at_ms > 0),
    source_revision TEXT CHECK (
        source_revision IS NULL OR char_length(source_revision) <= 255
    ),
    evidence_sha256 TEXT NOT NULL CHECK (
        evidence_sha256 ~ '^[a-f0-9]{64}$'
    ),
    normalized_content_sha256 TEXT CHECK (
        normalized_content_sha256 IS NULL
        OR normalized_content_sha256 ~ '^[a-f0-9]{64}$'
    ),
    state TEXT NOT NULL CHECK (
        state IN ('changed', 'unchanged', 'invalid')
    ),
    previous_snapshot_id UUID
        REFERENCES pricing_source_snapshots(snapshot_id) ON DELETE RESTRICT,
    snapshot_id UUID
        REFERENCES pricing_source_snapshots(snapshot_id) ON DELETE RESTRICT,
    failure_code TEXT CHECK (
        failure_code IS NULL
        OR (
            char_length(failure_code) BETWEEN 1 AND 128
            AND failure_code ~ '^[a-z0-9_]+$'
        )
    ),
    evidence_metadata JSONB NOT NULL CHECK (
        jsonb_typeof(evidence_metadata) = 'object'
    ),
    initiated_by_user_id UUID NOT NULL
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    created_at_ms BIGINT NOT NULL,
    completed_at_ms BIGINT NOT NULL,
    CHECK (completed_at_ms >= created_at_ms),
    CHECK (
        (
            state = 'invalid'
            AND snapshot_id IS NULL
            AND normalized_content_sha256 IS NULL
            AND failure_code IS NOT NULL
        )
        OR (
            state IN ('changed', 'unchanged')
            AND snapshot_id IS NOT NULL
            AND normalized_content_sha256 IS NOT NULL
            AND failure_code IS NULL
        )
    ),
    CHECK (
        previous_snapshot_id IS NULL
        OR snapshot_id IS NULL
        OR previous_snapshot_id <> snapshot_id
    )
);

CREATE INDEX pricing_source_sync_runs_catalog_created_idx
    ON pricing_source_sync_runs (
        catalog_key, created_at_ms DESC, sync_run_id DESC
    );

CREATE INDEX pricing_source_sync_runs_snapshot_idx
    ON pricing_source_sync_runs (
        snapshot_id, created_at_ms DESC, sync_run_id DESC
    )
    WHERE snapshot_id IS NOT NULL;

CREATE OR REPLACE FUNCTION preserve_pricing_source_snapshot_evidence()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'pricing source snapshots cannot be deleted'
            USING ERRCODE = '55000';
    END IF;

    IF ROW(
        OLD.snapshot_id, OLD.catalog_key, OLD.source_provider_id,
        OLD.currency, OLD.source_url, OLD.source_checked_at_ms,
        OLD.source_revision, OLD.parser_version, OLD.content_sha256,
        OLD.item_count, OLD.normalized_payload, OLD.created_by_user_id,
        OLD.created_at_ms
    ) IS DISTINCT FROM ROW(
        NEW.snapshot_id, NEW.catalog_key, NEW.source_provider_id,
        NEW.currency, NEW.source_url, NEW.source_checked_at_ms,
        NEW.source_revision, NEW.parser_version, NEW.content_sha256,
        NEW.item_count, NEW.normalized_payload, NEW.created_by_user_id,
        NEW.created_at_ms
    ) THEN
        RAISE EXCEPTION 'pricing source snapshot evidence is immutable'
            USING ERRCODE = '55000';
    END IF;

    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'observed'
            AND NEW.state IN ('partially_applied', 'applied', 'rejected'))
        OR (OLD.state = 'partially_applied'
            AND NEW.state IN ('applied', 'rejected'))
    ) OR NEW.updated_at_ms < OLD.updated_at_ms THEN
        RAISE EXCEPTION 'invalid pricing source snapshot transition'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pricing_source_snapshots_preserve_evidence
BEFORE UPDATE OR DELETE ON pricing_source_snapshots
FOR EACH ROW
EXECUTE FUNCTION preserve_pricing_source_snapshot_evidence();

CREATE OR REPLACE FUNCTION preserve_pricing_source_sync_run()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'pricing source sync runs are immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pricing_source_sync_runs_immutable
BEFORE UPDATE OR DELETE ON pricing_source_sync_runs
FOR EACH ROW
EXECUTE FUNCTION preserve_pricing_source_sync_run();
