ALTER TABLE provider_account_login_sessions
    ADD CONSTRAINT provider_account_login_sessions_authorization_url_valid CHECK (
        authorization_url IS NULL
        OR (
            char_length(authorization_url) BETWEEN 1 AND 8192
            AND authorization_url !~ '[[:cntrl:]]'
        )
    );
