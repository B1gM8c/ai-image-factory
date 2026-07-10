CREATE TABLE scheduler_scopes (
    scope_key TEXT PRIMARY KEY,
    weight INTEGER NOT NULL CHECK (weight > 0),
    next_finish_tag BIGINT NOT NULL DEFAULT 0 CHECK (next_finish_tag >= 0),
    updated_at_ms BIGINT NOT NULL
);

ALTER TABLE work_items
    ADD COLUMN schedule_scope TEXT NOT NULL DEFAULT 'legacy',
    ADD COLUMN schedule_weight INTEGER NOT NULL DEFAULT 1 CHECK (schedule_weight > 0),
    ADD COLUMN schedule_priority SMALLINT NOT NULL DEFAULT 1 CHECK (schedule_priority BETWEEN 0 AND 3),
    ADD COLUMN schedule_cost BIGINT NOT NULL DEFAULT 1 CHECK (schedule_cost > 0),
    ADD COLUMN schedule_finish_tag BIGINT NOT NULL DEFAULT 0 CHECK (schedule_finish_tag >= 0);

CREATE INDEX work_items_schedule_claim_idx
    ON work_items (schedule_finish_tag, schedule_priority DESC, created_at_ms, work_item_id)
    WHERE state = 'ready';
