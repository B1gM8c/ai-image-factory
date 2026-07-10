# Phase 1C API Key Hardening

This slice replaces plain SHA-256 storage for newly issued gateway keys with a
versioned HMAC-SHA-256 keyring. It is an identity-storage change; authorization
scopes, validity windows, and budget policies remain separate follow-up work.

## Credential Format

New credentials contain a public database key ID and a 256-bit random secret:

```text
sk-gw-key_<public-id>.<64-hex-secret>
```

Authentication uses the public ID for one indexed lookup and verifies
`HMAC-SHA-256(pepper_version, full_token)` in constant time. PostgreSQL stores
the algorithm and pepper version beside the digest. It never stores the secret
or pepper.

## Rotation

`GATEWAY_API_KEY_PEPPERS` supplies comma-separated `version:64-hex` entries.
`GATEWAY_API_KEY_CURRENT_PEPPER_VERSION` selects the version used for new
credentials. Rotation follows an overlap window:

1. deploy the new pepper alongside the old pepper;
2. make the new version current;
3. rotate or revoke credentials still using the old version;
4. remove the old pepper, which immediately rejects remaining old-version
   credentials.

Production startup fails closed when the keyring is missing, malformed, lacks
the current version, or contains a pepper that is not exactly 32 bytes.

## Migration And Concurrency

Migration `0004_api_key_hmac.sql` marks existing rows as legacy SHA-256 rows.
They remain readable during a controlled migration window only when
`GATEWAY_API_KEY_ALLOW_LEGACY_SHA256=true`; the default is fail-closed. Every
newly created row must carry `hmac-sha256-v1` and a positive pepper version.

Authentication takes a PostgreSQL `FOR NO KEY UPDATE` row lock while verifying
a credential. Revocation therefore has a clear serialization point and wins
when it was already queued first. Authentication is serialized only per key;
different keys remain independent. `last_used_at` is updated at most once per
minute per key to avoid a write and row version on every API request.

The keyring is never passed to Codex child processes, logs, traces, API
responses, or database rows.
