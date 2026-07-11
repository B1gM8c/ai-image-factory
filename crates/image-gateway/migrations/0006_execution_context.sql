ALTER TABLE quota_reservations
    ADD COLUMN limit_5h INTEGER CHECK (limit_5h >= 0),
    ADD COLUMN remaining_5h INTEGER CHECK (remaining_5h BETWEEN 0 AND limit_5h),
    ADD COLUMN limit_7d INTEGER CHECK (limit_7d >= 0),
    ADD COLUMN remaining_7d INTEGER CHECK (remaining_7d BETWEEN 0 AND limit_7d);

UPDATE quota_reservations qr
SET limit_5h = rp.limit_5h,
    remaining_5h = rp.remaining_5h,
    limit_7d = rp.limit_7d,
    remaining_7d = rp.remaining_7d
FROM job_response_projections rp
WHERE rp.job_id = qr.job_id;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM quota_reservations qr
        JOIN jobs j ON j.job_id = qr.job_id
        WHERE qr.state = 'reserved'
          AND j.state IN ('reserved', 'queued', 'running')
          AND (qr.limit_5h IS NULL
            OR qr.remaining_5h IS NULL
            OR qr.limit_7d IS NULL
            OR qr.remaining_7d IS NULL)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'check_violation',
            MESSAGE = 'migration 0006 requires active jobs to be drained before upgrade';
    END IF;
END $$;

ALTER TABLE quota_reservations
    ADD CONSTRAINT quota_reservations_snapshot_all_or_none_check CHECK (
        (limit_5h IS NULL AND remaining_5h IS NULL
            AND limit_7d IS NULL AND remaining_7d IS NULL)
        OR
        (limit_5h IS NOT NULL AND remaining_5h IS NOT NULL
            AND limit_7d IS NOT NULL AND remaining_7d IS NOT NULL)
    );
