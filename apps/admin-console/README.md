# AI Image Factory Admin Console

Next.js + React console for operating the image API platform.

```bash
npm install
npm run dev:admin
```

Environment:

```bash
GATEWAY_BASE_URL=http://127.0.0.1:8787
GATEWAY_ADMIN_TOKEN=admin-token
```

The console uses `/api/gateway/*` as a server-side proxy to the Rust gateway so
admin credentials stay out of browser JavaScript.
