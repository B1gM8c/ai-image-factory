CREATE TABLE project_webhook_outbox_receipts (
    outbox_event_id UUID PRIMARY KEY
        REFERENCES outbox_events(event_id) ON DELETE RESTRICT,
    processed_at_ms BIGINT NOT NULL
);

COMMENT ON TABLE project_webhook_outbox_receipts IS
    'Webhook-specific durable outbox consumer receipts; does not claim the shared outbox publication marker.';
