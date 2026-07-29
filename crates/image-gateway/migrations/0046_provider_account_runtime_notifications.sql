CREATE FUNCTION notify_provider_account_runtime_changed()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify(
        'ai_image_factory_provider_account_runtime',
        NEW.provider_account_id::TEXT
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER executor_resource_policies_runtime_changed
AFTER UPDATE OF allocated_count ON executor_resource_policies
FOR EACH ROW
WHEN (OLD.allocated_count IS DISTINCT FROM NEW.allocated_count)
EXECUTE FUNCTION notify_provider_account_runtime_changed();

CREATE TRIGGER provider_account_execution_controls_runtime_changed
AFTER UPDATE OF desired_max_concurrency, lifecycle_state
ON provider_account_execution_controls
FOR EACH ROW
WHEN (
    OLD.desired_max_concurrency IS DISTINCT FROM NEW.desired_max_concurrency
    OR OLD.lifecycle_state IS DISTINCT FROM NEW.lifecycle_state
)
EXECUTE FUNCTION notify_provider_account_runtime_changed();

COMMENT ON FUNCTION notify_provider_account_runtime_changed() IS
    'Emits a commit-ordered hint for the admin runtime event hub; consumers must re-read authoritative state.';
