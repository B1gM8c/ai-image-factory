# Admin Control Plane Bootstrap

This runbook creates the production-shaped Axum identity and Next.js BFF path. It keeps
administrator JWTs separate from data-plane API keys and gives admin read models a database role
that cannot write.

## Preconditions

- PostgreSQL is reachable through a migration-owner connection.
- TLS terminates before the loopback-only Gateway and Next.js processes.
- Runtime secrets come from a secret manager or private mounted files, never from the repository.
- `openssl`, Rust, Node.js, and npm are installed.

## 1. Build And Migrate

Run migrations with the schema owner before starting any application process:

```bash
export DATABASE_URL='postgresql://migration_owner@127.0.0.1:5432/ai_image_factory'
cargo build --locked -p gpt-image-2-gateway --bin factoryctl --bin gpt-image-2-gateway
./target/debug/factoryctl migrate
```

Application startup verifies the complete embedded migration set; it does not
run DDL. Deploy every service binary from the same commit as `factoryctl`.

## 2. Create Identity Material

Generate a named-curve P-256 key and a versioned refresh-token pepper file in a private absolute
directory:

```bash
./scripts/generate-admin-identity-secrets.sh \
  /var/lib/ai-image-factory/identity \
  admin-es256-v1
```

The script refuses relative paths, symlink output directories, and existing target files. It never
prints private key or pepper contents. Store the five reported settings in the Gateway runtime
configuration. For rotation, use a new directory or filenames, publish both public keys, switch the
active key ID, and retire the old public key only after all old access tokens have expired.

## 3. Create The Read-Only Database Role

Run the following as the database/schema owner. Replace the database and role names to match the
deployment. Provision the login password through the database secret workflow, not in this file.

```sql
CREATE ROLE aif_admin_reader LOGIN NOINHERIT;
GRANT CONNECT ON DATABASE ai_image_factory TO aif_admin_reader;
GRANT USAGE ON SCHEMA public TO aif_admin_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO aif_admin_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO aif_admin_reader;
```

`ALTER DEFAULT PRIVILEGES` must execute as the role that will own future migration-created tables.
The admin adapter also sets `default_transaction_read_only=on`, but that client setting is defense
in depth; PostgreSQL grants remain the authority.

Verify both sides of the boundary before release:

```sql
SET ROLE aif_admin_reader;
SELECT count(*) FROM jobs;
INSERT INTO jobs DEFAULT VALUES; -- must fail with insufficient_privilege
RESET ROLE;
```

## 4. Configure Axum Identity

The Gateway needs the primary database connection, the separate read connection, a data-plane
bootstrap token, API-key hashing material, and the identity settings generated above:

```bash
export DATABASE_URL='postgresql://gateway_writer@127.0.0.1:5432/ai_image_factory'
export GATEWAY_ADMIN_READ_DATABASE_URL='postgresql://aif_admin_reader@127.0.0.1:5432/ai_image_factory'
export GATEWAY_BIND='127.0.0.1:8787'

export GATEWAY_API_TOKEN='<data-plane-bootstrap-token>'
export GATEWAY_API_KEY_PEPPERS='1:<64-hex-characters>'
export GATEWAY_API_KEY_CURRENT_PEPPER_VERSION='1'

export GATEWAY_IDENTITY_ENABLED='true'
export GATEWAY_AUTH_ISSUER='https://api.example.com'
export GATEWAY_AUTH_AUDIENCE='ai-image-factory-admin'
export GATEWAY_AUTH_CLIENT_ID='ai-image-factory-admin-bff'
export GATEWAY_JWT_ACTIVE_KID='admin-es256-v1'
export GATEWAY_JWT_PRIVATE_KEY_PATH='/var/lib/ai-image-factory/identity/admin-jwt-es256-private.pem'
export GATEWAY_JWT_PUBLIC_KEYS='admin-es256-v1:/var/lib/ai-image-factory/identity/admin-jwt-es256-public.pem'
export GATEWAY_REFRESH_TOKEN_CURRENT_PEPPER_VERSION='1'
export GATEWAY_REFRESH_TOKEN_PEPPERS_PATH='/var/lib/ai-image-factory/identity/refresh-token-peppers'
```

`GATEWAY_API_TOKEN` is a data-plane compatibility bootstrap credential. It must not be reused as an
admin credential or exposed to the browser. Keep `GATEWAY_LEGACY_ADMIN_AUTH_ENABLED` unset or false
during normal operation.

## 5. Bootstrap The First Owner

Use the same database and identity environment as the Gateway. The command requires an interactive
TTY and reads the password twice without echoing it:

```bash
./target/debug/factoryctl bootstrap-admin owner@example.com 'Platform Owner'
```

Do not automate the initial password through an environment variable or command argument.

## 6. Start Gateway And Console

Start the Gateway after migration and bootstrap:

```bash
./target/debug/gpt-image-2-gateway
```

Start the console with the same BFF client ID and its exact public origin:

```bash
export PORT='3010'
export GATEWAY_BASE_URL='http://127.0.0.1:8787'
export ADMIN_CONSOLE_ORIGIN='https://admin.example.com'
export ADMIN_CONSOLE_CLIENT_ID='ai-image-factory-admin-bff'
npm run build:admin
npm run start:admin
```

Run both built processes through the deployment process supervisor. The browser receives only
HttpOnly access/refresh cookies plus a CSRF cookie. Every BFF mutation requires the exact configured
origin, `Sec-Fetch-Site: same-origin`, matching CSRF values, and an `application/json` media type.

## 7. Release Checks

```bash
curl --fail http://127.0.0.1:8787/healthz
curl --fail http://127.0.0.1:8787/readyz
curl --fail http://127.0.0.1:8787/openapi.json >/dev/null
```

Then verify through the console origin:

1. Login succeeds and `/overview` loads real PostgreSQL-backed values.
2. Refresh rotates the token and leaves exactly one active successor.
3. Logout revokes the session family and protected BFF reads return `401`.
4. A mutation without JSON content type returns `415`.
5. A cross-origin or CSRF-mismatched mutation returns `403`.
6. The read-only role can execute admin reads and cannot write any table.

Do not treat a port-open check as completion. Preserve the HTTP, cookie, and database evidence for
each release.
