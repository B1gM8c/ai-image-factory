# AI Image Factory Admin Console

Next.js + React console for operating the image API platform.

```bash
npm install
npm run dev:admin
```

Environment:

```bash
PORT=3010
GATEWAY_BASE_URL=http://127.0.0.1:8787
ADMIN_CONSOLE_ORIGIN=http://127.0.0.1:3010
ADMIN_CONSOLE_CLIENT_ID=ai-image-factory-admin-bff
```

The Gateway client ID must match `GATEWAY_AUTH_CLIENT_ID`. The console stores short-lived access
JWTs and rotating opaque refresh tokens only in HttpOnly cookies, and uses `/api/gateway/*` as an
allowlisted server-side proxy so credentials stay out of browser JavaScript.

Static `GATEWAY_ADMIN_TOKEN` authentication is not part of the normal console path. A local-only
emergency session requires both `ADMIN_CONSOLE_ACCESS_TOKEN` and `GATEWAY_ADMIN_TOKEN`; never set
them on the production console. See
[`docs/operations/admin-control-plane-bootstrap.md`](../../docs/operations/admin-control-plane-bootstrap.md)
for the complete database, identity, and BFF bootstrap sequence.
