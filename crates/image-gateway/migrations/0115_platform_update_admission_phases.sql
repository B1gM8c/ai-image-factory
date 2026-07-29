ALTER TABLE platform_update_commands
    DROP CONSTRAINT platform_update_commands_phase_check;

ALTER TABLE platform_update_commands
    ADD CONSTRAINT platform_update_commands_phase_check CHECK (
        phase IN (
            'queued', 'preflight', 'staged',
            'admission_closing', 'admission_closed',
            'quiescing', 'quiesced', 'recovery_ready',
            'migrated', 'switched', 'verified', 'activating_full',
            'restoring', 'restored', 'failed'
        )
    );
