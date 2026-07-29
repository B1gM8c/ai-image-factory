CREATE INDEX identity_audit_events_action_created_idx
    ON identity_audit_events (action, created_at_ms DESC, event_id DESC);

CREATE INDEX identity_audit_events_outcome_created_idx
    ON identity_audit_events (outcome, created_at_ms DESC, event_id DESC);

CREATE INDEX identity_audit_events_project_created_idx
    ON identity_audit_events (
        (
            COALESCE(
                CASE
                    WHEN resource_type = 'project' THEN resource_id
                    ELSE NULL
                END,
                metadata ->> 'project_id'
            )
        ),
        created_at_ms DESC,
        event_id DESC
    )
    WHERE (
        resource_type = 'project'
        OR metadata ? 'project_id'
    );
