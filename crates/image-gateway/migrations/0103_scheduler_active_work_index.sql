CREATE INDEX work_items_admin_active_created_idx
    ON work_items (
        (
            CASE
                WHEN state IN ('leased', 'running', 'awaiting_executor') THEN 0
                ELSE 1
            END
        ),
        created_at_ms DESC,
        job_id DESC
    )
    WHERE state IN ('ready', 'leased', 'running', 'awaiting_executor');
