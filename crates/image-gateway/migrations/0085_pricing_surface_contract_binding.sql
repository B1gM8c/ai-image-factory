CREATE TABLE pricing_surface_contract_revisions (
    contract_key TEXT NOT NULL CHECK (
        char_length(contract_key) BETWEEN 1 AND 192
        AND contract_key ~ '^[A-Za-z0-9_.:-]+$'
    ),
    revision BIGINT NOT NULL CHECK (revision > 0),
    contract_hash TEXT NOT NULL CHECK (
        contract_hash ~ '^[a-f0-9]{64}$'
    ),
    contract_schema_version INTEGER NOT NULL CHECK (
        contract_schema_version > 0
    ),
    api_profile TEXT NOT NULL CHECK (
        char_length(api_profile) BETWEEN 1 AND 128
        AND api_profile ~ '^[A-Za-z0-9_.-]+$'
    ),
    operation TEXT NOT NULL CHECK (
        char_length(operation) BETWEEN 1 AND 128
        AND operation ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_model_id TEXT NOT NULL CHECK (
        char_length(provider_model_id) BETWEEN 1 AND 255
        AND provider_model_id !~ '[[:cntrl:]]'
    ),
    public_model_id TEXT NOT NULL CHECK (
        char_length(public_model_id) BETWEEN 1 AND 255
        AND public_model_id !~ '[[:cntrl:]]'
    ),
    media_kind TEXT NOT NULL CHECK (media_kind IN ('image', 'video')),
    service_tier TEXT NOT NULL CHECK (
        char_length(service_tier) BETWEEN 1 AND 64
        AND service_tier ~ '^[A-Za-z0-9_.-]+$'
    ),
    execution_surface TEXT NOT NULL CHECK (
        execution_surface IN ('provider_api', 'provider_cli', 'manual_import')
    ),
    normalizer_key TEXT NOT NULL CHECK (
        char_length(normalizer_key) BETWEEN 1 AND 192
        AND normalizer_key ~ '^[A-Za-z0-9_.:-]+$'
    ),
    normalizer_revision BIGINT NOT NULL CHECK (normalizer_revision > 0),
    contract_json JSONB NOT NULL CHECK (
        jsonb_typeof(contract_json) = 'object'
    ),
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (contract_key, revision),
    UNIQUE (contract_key, revision, contract_hash)
);

COMMENT ON TABLE pricing_surface_contract_revisions IS
    'Immutable, route-independent PricingSurfaceContract snapshots. contract_hash is the lowercase SHA-256 of the canonical contract encoded by the application.';
COMMENT ON COLUMN pricing_surface_contract_revisions.contract_key IS
    'Stable logical surface identity; revisions of one key must retain the same exact provider/API/model surface.';
COMMENT ON COLUMN pricing_surface_contract_revisions.contract_json IS
    'Canonical exact contract snapshot, including request domains, constraints, metering bases, cardinality and support state.';

CREATE INDEX pricing_surface_contract_revisions_surface_idx
    ON pricing_surface_contract_revisions (
        api_profile, operation, provider_id, provider_model_id,
        public_model_id, media_kind, service_tier, execution_surface,
        revision DESC
    );

CREATE FUNCTION validate_pricing_surface_contract_revision()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('pricing-surface-contract:' || NEW.contract_key, 0)
    );

    IF EXISTS (
        SELECT 1
        FROM pricing_surface_contract_revisions existing
        WHERE existing.contract_key = NEW.contract_key
          AND existing.revision = NEW.revision
          AND existing.contract_hash = NEW.contract_hash
          AND ROW(
              existing.contract_schema_version,
              existing.api_profile, existing.operation,
              existing.provider_id, existing.provider_model_id,
              existing.public_model_id, existing.media_kind,
              existing.service_tier, existing.execution_surface,
              existing.normalizer_key, existing.normalizer_revision,
              existing.contract_json
          ) IS NOT DISTINCT FROM ROW(
              NEW.contract_schema_version,
              NEW.api_profile, NEW.operation,
              NEW.provider_id, NEW.provider_model_id,
              NEW.public_model_id, NEW.media_kind,
              NEW.service_tier, NEW.execution_surface,
              NEW.normalizer_key, NEW.normalizer_revision,
              NEW.contract_json
          )
    ) THEN
        RETURN NEW;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pricing_surface_contract_revisions existing
        WHERE existing.contract_key = NEW.contract_key
          AND ROW(
              existing.api_profile, existing.operation,
              existing.provider_id, existing.provider_model_id,
              existing.public_model_id, existing.media_kind,
              existing.service_tier, existing.execution_surface
          ) IS DISTINCT FROM ROW(
              NEW.api_profile, NEW.operation,
              NEW.provider_id, NEW.provider_model_id,
              NEW.public_model_id, NEW.media_kind,
              NEW.service_tier, NEW.execution_surface
          )
    ) THEN
        RAISE EXCEPTION
            'pricing surface contract key cannot change surface identity'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pricing_surface_contract_revisions existing
        WHERE existing.contract_key = NEW.contract_key
          AND existing.revision >= NEW.revision
    ) THEN
        RAISE EXCEPTION
            'pricing surface contract revisions must increase monotonically'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pricing_surface_contract_revisions_validate_insert
BEFORE INSERT ON pricing_surface_contract_revisions
FOR EACH ROW EXECUTE FUNCTION validate_pricing_surface_contract_revision();

CREATE FUNCTION reject_pricing_surface_contract_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'pricing surface contract revisions are immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pricing_surface_contract_revisions_immutable
BEFORE UPDATE OR DELETE ON pricing_surface_contract_revisions
FOR EACH ROW EXECUTE FUNCTION reject_pricing_surface_contract_mutation();

CREATE TRIGGER pricing_surface_contract_revisions_reject_truncate
BEFORE TRUNCATE ON pricing_surface_contract_revisions
FOR EACH STATEMENT EXECUTE FUNCTION reject_pricing_surface_contract_mutation();

CREATE TABLE price_book_version_surface_contract_bindings (
    price_book_version_id UUID NOT NULL
        REFERENCES price_book_versions(price_book_version_id)
        ON DELETE RESTRICT,
    contract_key TEXT NOT NULL,
    contract_revision BIGINT NOT NULL CHECK (contract_revision > 0),
    contract_hash TEXT NOT NULL CHECK (
        contract_hash ~ '^[a-f0-9]{64}$'
    ),
    bound_at_ms BIGINT NOT NULL,
    PRIMARY KEY (price_book_version_id, contract_key),
    FOREIGN KEY (contract_key, contract_revision, contract_hash)
        REFERENCES pricing_surface_contract_revisions(
            contract_key, revision, contract_hash
        )
        ON DELETE RESTRICT
);

COMMENT ON TABLE price_book_version_surface_contract_bindings IS
    'Exact contract revision/hash snapshots selected while a customer-sale price version is still a draft.';
COMMENT ON COLUMN price_book_version_surface_contract_bindings.contract_hash IS
    'Repeated deliberately so the published price version audit row names the exact hash and the composite foreign key verifies it.';

CREATE INDEX price_book_version_surface_contract_bindings_contract_idx
    ON price_book_version_surface_contract_bindings (
        contract_key, contract_revision, contract_hash,
        price_book_version_id
    );

CREATE FUNCTION pricing_surface_contract_matches_price_version(
    version_row price_book_versions,
    book_provider_id_value TEXT,
    contract_row pricing_surface_contract_revisions
) RETURNS BOOLEAN AS $$
    SELECT (
        (
            version_row.api_profile = '*'
            OR version_row.api_profile = contract_row.api_profile
            OR EXISTS (
                SELECT 1
                FROM api_profile_pricing_aliases alias
                WHERE alias.api_profile = contract_row.api_profile
                  AND alias.pricing_api_profile =
                      version_row.api_profile
            )
        )
        AND version_row.operation IN ('*', contract_row.operation)
        AND (
            book_provider_id_value IS NULL
            OR book_provider_id_value = contract_row.provider_id
        )
        AND (
            version_row.provider_id IS NULL
            OR version_row.provider_id = contract_row.provider_id
        )
        AND (
            version_row.provider_model_id IS NULL
            OR version_row.provider_model_id =
                contract_row.provider_model_id
        )
        AND version_row.public_model_id IN (
            '*', contract_row.public_model_id
        )
        AND version_row.media_kind = contract_row.media_kind
        AND version_row.service_tier IN (
            '*', contract_row.service_tier
        )
        AND version_row.execution_surface =
            contract_row.execution_surface
    );
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION preserve_price_book_surface_contract_binding()
RETURNS TRIGGER AS $$
DECLARE
    book_purpose TEXT;
    book_provider_id TEXT;
    parent_version price_book_versions%ROWTYPE;
    contract pricing_surface_contract_revisions%ROWTYPE;
    duplicate_identity_exists BOOLEAN;
    previous_contract_key TEXT;
BEGIN
    IF TG_OP = 'UPDATE'
       AND NEW.price_book_version_id <> OLD.price_book_version_id THEN
        RAISE EXCEPTION 'surface contract binding parent is immutable'
            USING ERRCODE = '55000';
    END IF;

    SELECT version.* INTO STRICT parent_version
    FROM price_book_versions version
    WHERE version.price_book_version_id =
        CASE WHEN TG_OP = 'DELETE'
             THEN OLD.price_book_version_id
             ELSE NEW.price_book_version_id
        END
    FOR UPDATE OF version;

    IF parent_version.state <> 'draft' THEN
        RAISE EXCEPTION 'published surface contract binding is immutable'
            USING ERRCODE = '55000';
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    SELECT book.purpose, book.provider_id
      INTO STRICT book_purpose, book_provider_id
    FROM price_books book
    WHERE book.price_book_id = parent_version.price_book_id;

    IF book_purpose <> 'customer_sale' THEN
        RAISE EXCEPTION
            'surface contracts may bind only customer-sale price versions'
            USING ERRCODE = '23514';
    END IF;

    SELECT * INTO STRICT contract
    FROM pricing_surface_contract_revisions
    WHERE contract_key = NEW.contract_key
      AND revision = NEW.contract_revision
      AND contract_hash = NEW.contract_hash;

    IF NOT pricing_surface_contract_matches_price_version(
        parent_version, book_provider_id, contract
    ) THEN
        RAISE EXCEPTION
            'surface contract does not match price version selector'
            USING ERRCODE = '23514';
    END IF;

    previous_contract_key :=
        CASE WHEN TG_OP = 'UPDATE' THEN OLD.contract_key END;

    SELECT EXISTS (
        SELECT 1
        FROM price_book_version_surface_contract_bindings binding
        JOIN pricing_surface_contract_revisions existing
          ON existing.contract_key = binding.contract_key
         AND existing.revision = binding.contract_revision
         AND existing.contract_hash = binding.contract_hash
        WHERE binding.price_book_version_id =
              NEW.price_book_version_id
          AND ROW(
              existing.api_profile, existing.operation,
              existing.provider_id, existing.provider_model_id,
              existing.public_model_id, existing.media_kind,
              existing.service_tier, existing.execution_surface
          ) IS NOT DISTINCT FROM ROW(
              contract.api_profile, contract.operation,
              contract.provider_id, contract.provider_model_id,
              contract.public_model_id, contract.media_kind,
              contract.service_tier, contract.execution_surface
          )
          AND (
              previous_contract_key IS NULL
              OR binding.contract_key <> previous_contract_key
          )
    ) INTO duplicate_identity_exists;

    IF duplicate_identity_exists THEN
        RAISE EXCEPTION
            'price version already binds this exact surface identity'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER price_book_version_surface_contract_bindings_preserve
BEFORE INSERT OR UPDATE OR DELETE
ON price_book_version_surface_contract_bindings
FOR EACH ROW EXECUTE FUNCTION preserve_price_book_surface_contract_binding();

CREATE TRIGGER price_book_version_surface_contract_bindings_reject_truncate
BEFORE TRUNCATE ON price_book_version_surface_contract_bindings
FOR EACH STATEMENT EXECUTE FUNCTION reject_pricing_surface_contract_mutation();

CREATE FUNCTION require_surface_contract_for_customer_sale_publish()
RETURNS TRIGGER AS $$
DECLARE
    book_purpose TEXT;
    book_provider_id TEXT;
BEGIN
    IF NEW.state = 'active'
       AND (
           TG_OP = 'INSERT'
           OR (TG_OP = 'UPDATE' AND OLD.state = 'draft')
       ) THEN
        SELECT book.purpose, book.provider_id
          INTO STRICT book_purpose, book_provider_id
        FROM price_books book
        WHERE book.price_book_id = NEW.price_book_id;

        IF book_purpose = 'customer_sale'
           AND NOT EXISTS (
               SELECT 1
               FROM price_book_version_surface_contract_bindings binding
               WHERE binding.price_book_version_id =
                     NEW.price_book_version_id
           ) THEN
            RAISE EXCEPTION
                'customer-sale price publication requires a surface contract'
                USING ERRCODE = '23514';
        END IF;

        IF book_purpose = 'customer_sale'
           AND EXISTS (
               SELECT 1
               FROM price_book_version_surface_contract_bindings binding
               JOIN pricing_surface_contract_revisions contract
                 ON contract.contract_key = binding.contract_key
                AND contract.revision = binding.contract_revision
                AND contract.contract_hash = binding.contract_hash
               WHERE binding.price_book_version_id =
                     NEW.price_book_version_id
                 AND NOT pricing_surface_contract_matches_price_version(
                     NEW, book_provider_id, contract
                 )
           ) THEN
            RAISE EXCEPTION
                'bound surface contract does not match final price version selector'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER price_book_versions_require_surface_contract_on_publish
BEFORE INSERT OR UPDATE ON price_book_versions
FOR EACH ROW
EXECUTE FUNCTION require_surface_contract_for_customer_sale_publish();

CREATE FUNCTION require_customer_quote_surface_contract()
RETURNS TRIGGER AS $$
BEGIN
    -- Active versions that predate this migration have no trustworthy contract
    -- snapshot. Keep them operational; every newly published customer-sale
    -- version is forced through the binding trigger above.
    IF NOT EXISTS (
        SELECT 1
        FROM price_book_version_surface_contract_bindings binding
        WHERE binding.price_book_version_id = NEW.price_book_version_id
    ) THEN
        RETURN NEW;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM price_book_version_surface_contract_bindings binding
        JOIN pricing_surface_contract_revisions contract
          ON contract.contract_key = binding.contract_key
         AND contract.revision = binding.contract_revision
         AND contract.contract_hash = binding.contract_hash
        WHERE binding.price_book_version_id =
              NEW.price_book_version_id
          AND contract.api_profile = NEW.api_profile
          AND contract.operation = NEW.operation
          AND contract.provider_id = NEW.provider_id
          AND contract.provider_model_id = NEW.provider_model_id
          AND contract.public_model_id = NEW.public_model_id
          AND contract.media_kind = NEW.media_kind
          AND contract.service_tier = NEW.service_tier
          AND contract.execution_surface = NEW.execution_surface
    ) THEN
        RAISE EXCEPTION
            'customer quote is not covered by a bound surface contract'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customer_price_quotes_require_surface_contract
BEFORE INSERT ON customer_price_quotes
FOR EACH ROW EXECUTE FUNCTION require_customer_quote_surface_contract();
