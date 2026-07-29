CREATE FUNCTION notify_provider_queue_runtime_changed()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('ai_image_factory_provider_account_runtime', '*');
    RETURN NEW;
END;
$$;

CREATE TRIGGER work_items_queue_inserted
AFTER INSERT ON work_items
FOR EACH ROW
WHEN (NEW.state = 'ready')
EXECUTE FUNCTION notify_provider_queue_runtime_changed();

CREATE TRIGGER work_items_queue_membership_changed
AFTER UPDATE OF state ON work_items
FOR EACH ROW
WHEN ((OLD.state = 'ready') IS DISTINCT FROM (NEW.state = 'ready'))
EXECUTE FUNCTION notify_provider_queue_runtime_changed();

CREATE TRIGGER project_batch_requests_queue_inserted
AFTER INSERT ON project_batch_requests
FOR EACH ROW
WHEN (NEW.state = 'pending')
EXECUTE FUNCTION notify_provider_queue_runtime_changed();

CREATE TRIGGER project_batch_requests_queue_membership_changed
AFTER UPDATE OF state ON project_batch_requests
FOR EACH ROW
WHEN ((OLD.state = 'pending') IS DISTINCT FROM (NEW.state = 'pending'))
EXECUTE FUNCTION notify_provider_queue_runtime_changed();

COMMENT ON FUNCTION notify_provider_queue_runtime_changed() IS
    'Emits a coalesced full-snapshot hint when immediate or Batch queue pressure changes.';
