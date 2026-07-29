ALTER TABLE provider_account_login_sessions
    RENAME COLUMN verification_url TO authorization_url;

ALTER TABLE provider_account_login_sessions
    ADD COLUMN login_method TEXT NOT NULL DEFAULT 'device_code' CHECK (
        login_method IN ('browser_oauth', 'device_code')
    );

ALTER TABLE provider_account_login_sessions
    ALTER COLUMN login_method DROP DEFAULT;
