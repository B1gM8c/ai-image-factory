CREATE TABLE price_books (
    price_book_id UUID PRIMARY KEY,
    price_book_key TEXT NOT NULL UNIQUE CHECK (
        char_length(price_book_key) BETWEEN 1 AND 128
        AND price_book_key ~ '^[A-Za-z0-9_.:-]+$'
    ),
    display_name TEXT NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 255),
    purpose TEXT NOT NULL CHECK (
        purpose IN (
            'customer_sale',
            'provider_actual',
            'provider_estimated',
            'provider_allocated',
            'provider_benchmark'
        )
    ),
    scope_type TEXT NOT NULL CHECK (
        scope_type IN ('platform', 'organization', 'project')
    ),
    organization_id TEXT
        REFERENCES identity_organizations(organization_id) ON DELETE RESTRICT,
    project_id TEXT,
    provider_id TEXT CHECK (
        provider_id IS NULL
        OR (
            char_length(provider_id) BETWEEN 1 AND 128
            AND provider_id ~ '^[A-Za-z0-9_.-]+$'
        )
    ),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    state TEXT NOT NULL CHECK (state IN ('active', 'archived')),
    control_version BIGINT NOT NULL DEFAULT 1 CHECK (control_version > 0),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (project_id, organization_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT,
    CHECK (
        (scope_type = 'platform'
            AND organization_id IS NULL AND project_id IS NULL)
        OR (scope_type = 'organization'
            AND organization_id IS NOT NULL AND project_id IS NULL)
        OR (scope_type = 'project'
            AND organization_id IS NOT NULL AND project_id IS NOT NULL)
    ),
    CHECK (
        purpose = 'customer_sale'
        OR provider_id IS NOT NULL
    ),
    CHECK (updated_at_ms >= created_at_ms)
);

CREATE INDEX price_books_resolution_idx
    ON price_books (
        purpose, scope_type, organization_id, project_id,
        provider_id, currency, state, price_book_key
    );

CREATE TABLE price_book_versions (
    price_book_version_id UUID PRIMARY KEY,
    price_book_id UUID NOT NULL
        REFERENCES price_books(price_book_id) ON DELETE RESTRICT,
    version INTEGER NOT NULL CHECK (version > 0),
    api_profile TEXT NOT NULL CHECK (
        char_length(api_profile) BETWEEN 1 AND 128
        AND api_profile ~ '^[A-Za-z0-9_.*-]+$'
    ),
    operation TEXT NOT NULL CHECK (
        char_length(operation) BETWEEN 1 AND 128
        AND operation ~ '^[A-Za-z0-9_.*-]+$'
    ),
    provider_id TEXT CHECK (
        provider_id IS NULL
        OR (
            char_length(provider_id) BETWEEN 1 AND 128
            AND provider_id ~ '^[A-Za-z0-9_.-]+$'
        )
    ),
    provider_model_id TEXT CHECK (
        provider_model_id IS NULL
        OR (
            char_length(provider_model_id) BETWEEN 1 AND 255
            AND provider_model_id !~ '[[:cntrl:]]'
        )
    ),
    public_model_id TEXT NOT NULL CHECK (
        char_length(public_model_id) BETWEEN 1 AND 255
        AND public_model_id !~ '[[:cntrl:]]'
    ),
    media_kind TEXT NOT NULL CHECK (media_kind IN ('image', 'video')),
    service_tier TEXT NOT NULL DEFAULT 'standard' CHECK (
        char_length(service_tier) BETWEEN 1 AND 64
        AND service_tier ~ '^[A-Za-z0-9_.-]+$'
    ),
    execution_surface TEXT NOT NULL CHECK (
        execution_surface IN ('provider_api', 'provider_cli', 'manual_import')
    ),
    billing_mode TEXT NOT NULL CHECK (
        billing_mode IN (
            'customer_rate',
            'provider_reported',
            'published_rate',
            'contract_rate',
            'subscription_allocation',
            'membership_points'
        )
    ),
    is_free BOOLEAN NOT NULL DEFAULT FALSE,
    state TEXT NOT NULL CHECK (state IN ('draft', 'active', 'retired')),
    effective_from_ms BIGINT NOT NULL,
    effective_until_ms BIGINT,
    source_kind TEXT NOT NULL CHECK (
        source_kind IN (
            'manual',
            'official_document',
            'provider_contract',
            'imported'
        )
    ),
    source_url TEXT CHECK (
        source_url IS NULL OR char_length(source_url) BETWEEN 1 AND 2048
    ),
    source_checked_at_ms BIGINT,
    notes TEXT CHECK (notes IS NULL OR char_length(notes) <= 4096),
    control_version BIGINT NOT NULL DEFAULT 1 CHECK (control_version > 0),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (price_book_id, version),
    UNIQUE (price_book_version_id, price_book_id),
    CHECK (
        effective_until_ms IS NULL OR effective_until_ms > effective_from_ms
    ),
    CHECK (
        source_kind NOT IN ('official_document', 'provider_contract')
        OR (source_url IS NOT NULL AND source_checked_at_ms IS NOT NULL)
    ),
    CHECK (updated_at_ms >= created_at_ms)
);

CREATE UNIQUE INDEX price_book_versions_active_match_uidx
    ON price_book_versions (
        price_book_id, api_profile, operation,
        COALESCE(provider_id, ''), COALESCE(provider_model_id, ''),
        public_model_id, media_kind, service_tier,
        execution_surface, billing_mode
    )
    WHERE state = 'active';

CREATE INDEX price_book_versions_catalog_idx
    ON price_book_versions (
        price_book_id, state, media_kind, public_model_id,
        effective_from_ms DESC, version DESC
    );

CREATE TABLE price_components (
    price_component_id UUID PRIMARY KEY,
    price_book_version_id UUID NOT NULL
        REFERENCES price_book_versions(price_book_version_id) ON DELETE RESTRICT,
    component_key TEXT NOT NULL CHECK (
        char_length(component_key) BETWEEN 1 AND 128
        AND component_key ~ '^[A-Za-z0-9_.:-]+$'
    ),
    metric TEXT NOT NULL CHECK (
        metric IN (
            'request',
            'image_input',
            'image_output',
            'text_input_token',
            'cached_text_input_token',
            'image_input_token',
            'cached_image_input_token',
            'image_output_token',
            'video_input_token',
            'video_output_token',
            'video_input_second',
            'video_output_second',
            'membership_point'
        )
    ),
    unit TEXT NOT NULL CHECK (
        unit IN ('request', 'image', 'token', 'second', 'point')
    ),
    unit_size BIGINT NOT NULL CHECK (unit_size > 0),
    unit_price_micros BIGINT NOT NULL CHECK (unit_price_micros >= 0),
    outcome TEXT NOT NULL CHECK (
        outcome IN ('succeeded', 'failed', 'no_effect', 'any')
    ),
    quantity_source TEXT NOT NULL CHECK (
        quantity_source IN (
            'provider_reported',
            'request_derived',
            'official_lookup',
            'operator_adjustment'
        )
    ),
    required_confidence TEXT NOT NULL DEFAULT 'exact' CHECK (
        required_confidence IN ('exact', 'bounded', 'estimated', 'any')
    ),
    rounding_mode TEXT NOT NULL CHECK (
        rounding_mode IN ('ceil', 'floor', 'half_up', 'exact')
    ),
    dimensions_json JSONB NOT NULL DEFAULT '{}'::JSONB CHECK (
        jsonb_typeof(dimensions_json) = 'object'
    ),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (price_book_version_id, component_key),
    CHECK (
        (metric = 'request' AND unit = 'request')
        OR (metric IN ('image_input', 'image_output') AND unit = 'image')
        OR (
            metric IN (
                'text_input_token',
                'cached_text_input_token',
                'image_input_token',
                'cached_image_input_token',
                'image_output_token',
                'video_input_token',
                'video_output_token'
            )
            AND unit = 'token'
        )
        OR (
            metric IN ('video_input_second', 'video_output_second')
            AND unit = 'second'
        )
        OR (metric = 'membership_point' AND unit = 'point')
    )
);

CREATE INDEX price_components_rating_idx
    ON price_components (
        price_book_version_id, metric, unit, outcome,
        quantity_source, required_confidence, component_key
    );

CREATE TABLE provider_usage_facts (
    usage_fact_id UUID PRIMARY KEY,
    semantic_key TEXT NOT NULL UNIQUE CHECK (
        char_length(semantic_key) BETWEEN 1 AND 512
        AND semantic_key !~ '[[:cntrl:]]'
    ),
    job_id UUID NOT NULL,
    output_id UUID NOT NULL,
    submission_id UUID NOT NULL,
    receipt_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    provider_account_id UUID,
    execution_surface TEXT NOT NULL CHECK (
        execution_surface IN ('provider_api', 'provider_cli', 'manual_import')
    ),
    metric TEXT NOT NULL CHECK (
        metric IN (
            'request',
            'image_input',
            'image_output',
            'text_input_token',
            'cached_text_input_token',
            'image_input_token',
            'cached_image_input_token',
            'image_output_token',
            'video_input_token',
            'video_output_token',
            'video_input_second',
            'video_output_second',
            'membership_point',
            'provider_reported_cost'
        )
    ),
    quantity BIGINT NOT NULL CHECK (quantity >= 0),
    unit TEXT NOT NULL CHECK (
        unit IN ('request', 'image', 'token', 'second', 'point', 'usd_tick')
    ),
    quantity_source TEXT NOT NULL CHECK (
        quantity_source IN (
            'provider_reported',
            'request_derived',
            'official_lookup',
            'operator_adjustment'
        )
    ),
    confidence TEXT NOT NULL CHECK (
        confidence IN ('exact', 'bounded', 'estimated')
    ),
    evidence_path TEXT CHECK (
        evidence_path IS NULL
        OR (
            char_length(evidence_path) BETWEEN 1 AND 512
            AND evidence_path !~ '[[:cntrl:]]'
        )
    ),
    metadata_json JSONB NOT NULL DEFAULT '{}'::JSONB CHECK (
        jsonb_typeof(metadata_json) = 'object'
    ),
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (receipt_id, submission_id, output_id, job_id)
        REFERENCES provider_receipts(
            receipt_id, submission_id, output_id, job_id
        ) ON DELETE RESTRICT,
    FOREIGN KEY (submission_id, output_id, job_id, provider_id)
        REFERENCES provider_submissions(
            submission_id, output_id, job_id, provider_id
        ) ON DELETE RESTRICT,
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_accounts(provider_account_id, provider_id)
        ON DELETE RESTRICT,
    CHECK (
        (metric = 'request' AND unit = 'request')
        OR (metric IN ('image_input', 'image_output') AND unit = 'image')
        OR (
            metric IN (
                'text_input_token',
                'cached_text_input_token',
                'image_input_token',
                'cached_image_input_token',
                'image_output_token',
                'video_input_token',
                'video_output_token'
            )
            AND unit = 'token'
        )
        OR (
            metric IN ('video_input_second', 'video_output_second')
            AND unit = 'second'
        )
        OR (metric = 'membership_point' AND unit = 'point')
        OR (metric = 'provider_reported_cost' AND unit = 'usd_tick')
    ),
    CHECK (
        (metric = 'provider_reported_cost' AND unit = 'usd_tick'
            AND quantity_source = 'provider_reported')
        OR metric <> 'provider_reported_cost'
    )
);

CREATE INDEX provider_usage_facts_job_idx
    ON provider_usage_facts (job_id, output_id, metric, usage_fact_id);

CREATE INDEX provider_usage_facts_provider_idx
    ON provider_usage_facts (
        provider_id, provider_account_id, metric, created_at_ms DESC
    );

CREATE FUNCTION preserve_price_book_version() RETURNS TRIGGER AS $$
DECLARE
    book_purpose TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state <> 'draft' THEN
            RAISE EXCEPTION 'published price book version is immutable'
                USING ERRCODE = '55000';
        END IF;
        RETURN OLD;
    END IF;

    IF OLD.state = 'draft' THEN
        IF NEW.state NOT IN ('draft', 'active') THEN
            RAISE EXCEPTION 'invalid price book version transition'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.state = 'active' THEN
            SELECT purpose INTO STRICT book_purpose
            FROM price_books
            WHERE price_book_id = NEW.price_book_id;

            IF NOT (
                (book_purpose = 'customer_sale'
                    AND NEW.billing_mode = 'customer_rate')
                OR (book_purpose = 'provider_actual'
                    AND NEW.billing_mode IN ('provider_reported', 'contract_rate'))
                OR (book_purpose = 'provider_estimated'
                    AND NEW.billing_mode IN ('published_rate', 'contract_rate'))
                OR (book_purpose = 'provider_allocated'
                    AND NEW.billing_mode IN (
                        'subscription_allocation', 'membership_points'
                    ))
                OR (book_purpose = 'provider_benchmark'
                    AND NEW.billing_mode = 'published_rate')
            ) THEN
                RAISE EXCEPTION 'price book purpose and billing mode are incompatible'
                    USING ERRCODE = '23514';
            END IF;

            IF NEW.billing_mode = 'provider_reported' THEN
                IF NEW.is_free OR EXISTS (
                    SELECT 1 FROM price_components
                    WHERE price_book_version_id = NEW.price_book_version_id
                ) THEN
                    RAISE EXCEPTION 'provider-reported cost versions cannot contain rates'
                        USING ERRCODE = '23514';
                END IF;
            ELSE
                IF NOT EXISTS (
                    SELECT 1 FROM price_components
                    WHERE price_book_version_id = NEW.price_book_version_id
                ) THEN
                    RAISE EXCEPTION 'priced versions require components'
                        USING ERRCODE = '23514';
                END IF;
                IF NEW.is_free AND EXISTS (
                    SELECT 1 FROM price_components
                    WHERE price_book_version_id = NEW.price_book_version_id
                      AND unit_price_micros <> 0
                ) THEN
                    RAISE EXCEPTION 'free versions require zero-valued components'
                        USING ERRCODE = '23514';
                END IF;
                IF NOT NEW.is_free AND NOT EXISTS (
                    SELECT 1 FROM price_components
                    WHERE price_book_version_id = NEW.price_book_version_id
                      AND unit_price_micros > 0
                ) THEN
                    RAISE EXCEPTION 'paid versions require a positive component'
                        USING ERRCODE = '23514';
                END IF;
                IF book_purpose = 'provider_actual'
                   AND EXISTS (
                       SELECT 1 FROM price_components
                       WHERE price_book_version_id = NEW.price_book_version_id
                         AND (
                             quantity_source <> 'provider_reported'
                             OR required_confidence <> 'exact'
                         )
                   ) THEN
                    RAISE EXCEPTION 'provider actual rates require exact provider usage'
                        USING ERRCODE = '23514';
                END IF;
            END IF;
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'active'
       AND NEW.state = 'retired'
       AND ROW(
           OLD.price_book_id, OLD.version, OLD.api_profile, OLD.operation,
           OLD.provider_id, OLD.provider_model_id, OLD.public_model_id,
           OLD.media_kind, OLD.service_tier, OLD.execution_surface,
           OLD.billing_mode, OLD.is_free, OLD.effective_from_ms,
           OLD.source_kind, OLD.source_url, OLD.source_checked_at_ms,
           OLD.notes, OLD.created_at_ms
       ) IS NOT DISTINCT FROM ROW(
           NEW.price_book_id, NEW.version, NEW.api_profile, NEW.operation,
           NEW.provider_id, NEW.provider_model_id, NEW.public_model_id,
           NEW.media_kind, NEW.service_tier, NEW.execution_surface,
           NEW.billing_mode, NEW.is_free, NEW.effective_from_ms,
           NEW.source_kind, NEW.source_url, NEW.source_checked_at_ms,
           NEW.notes, NEW.created_at_ms
       ) THEN
        RETURN NEW;
    END IF;

    IF ROW(OLD.*) IS DISTINCT FROM ROW(NEW.*) THEN
        RAISE EXCEPTION 'published price book version is immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER price_book_versions_preserve_published
BEFORE UPDATE OR DELETE ON price_book_versions
FOR EACH ROW EXECUTE FUNCTION preserve_price_book_version();

CREATE FUNCTION preserve_price_book_semantics() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM price_book_versions
        WHERE price_book_id = OLD.price_book_id
          AND state IN ('active', 'retired')
    )
    AND ROW(
        OLD.price_book_key, OLD.purpose, OLD.scope_type,
        OLD.organization_id, OLD.project_id, OLD.provider_id, OLD.currency,
        OLD.created_at_ms
    ) IS DISTINCT FROM ROW(
        NEW.price_book_key, NEW.purpose, NEW.scope_type,
        NEW.organization_id, NEW.project_id, NEW.provider_id, NEW.currency,
        NEW.created_at_ms
    ) THEN
        RAISE EXCEPTION 'published price book semantics are immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER price_books_preserve_published_semantics
BEFORE UPDATE ON price_books
FOR EACH ROW EXECUTE FUNCTION preserve_price_book_semantics();

CREATE FUNCTION preserve_price_component() RETURNS TRIGGER AS $$
DECLARE
    old_parent_state TEXT;
    new_parent_state TEXT;
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        SELECT state INTO STRICT old_parent_state
        FROM price_book_versions
        WHERE price_book_version_id = OLD.price_book_version_id;

        IF old_parent_state <> 'draft' THEN
            RAISE EXCEPTION 'published price component is immutable'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        SELECT state INTO STRICT new_parent_state
        FROM price_book_versions
        WHERE price_book_version_id = NEW.price_book_version_id;

        IF new_parent_state <> 'draft' THEN
            RAISE EXCEPTION 'published price component is immutable'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    IF TG_OP = 'UPDATE'
       AND NEW.price_book_version_id <> OLD.price_book_version_id THEN
        RAISE EXCEPTION 'price component parent is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER price_components_preserve_published
BEFORE INSERT OR UPDATE OR DELETE ON price_components
FOR EACH ROW EXECUTE FUNCTION preserve_price_component();

CREATE TRIGGER provider_usage_facts_reject_mutation
BEFORE UPDATE OR DELETE ON provider_usage_facts
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER provider_usage_facts_reject_truncate
BEFORE TRUNCATE ON provider_usage_facts
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();
