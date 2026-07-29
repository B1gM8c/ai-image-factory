SET LOCAL lock_timeout = '5s';

-- One provider command envelope may carry multiple immutable operations. The
-- operation descriptor, rather than a synthetic schema name, is the routing
-- and execution boundary.
ALTER TABLE provider_execution_profiles
    DROP CONSTRAINT provider_execution_profiles_provider_id_command_schema_adap_key;

ALTER TABLE provider_execution_profiles
    ADD CONSTRAINT provider_execution_profiles_operation_binding_unique
    UNIQUE (
        provider_id, operation_id, command_schema, adapter_revision,
        credential_pool_id, provider_account_id, credential_ref,
        credential_revision, resource_policy_id, resource_policy_revision
    );
