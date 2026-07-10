# AI Image Factory

AI Image Factory is a monorepo for an OpenAI-compatible image API platform.

The first active backend is the existing native Codex CLI path for `gpt-image-2`.
The platform layout now leaves explicit room for additional providers such as
Midjourney, JiMeng CLI, and Grok CLI without changing the public Images API.

## Workspace

```text
apps/
  admin-console/      Next.js + React + shadcn-style operations console
crates/
  image-gateway/      Rust Axum API gateway and OpenAI Images compatibility layer
  provider-contracts/ Shared media/provider/job contracts and roadmap slots
  scheduler-policy/  Provider-neutral weighted scheduling policy
docs/
  architecture/       Upgrade notes and platform boundaries
```

The authoritative target design is
[`docs/architecture/2026-ai-image-factory-target-architecture.md`](docs/architecture/2026-ai-image-factory-target-architecture.md).

## Common Commands

```bash
cargo test --workspace
cargo run -p gpt-image-2-gateway
npm install
npm run typecheck:admin
npm run dev:admin
npm run smoke:codex
```

The admin console proxies gateway requests through `/api/gateway/*`.
Set `GATEWAY_BASE_URL`, `GATEWAY_ADMIN_TOKEN`, and
`ADMIN_CONSOLE_ACCESS_TOKEN` for server-side admin calls. The console BFF
requires `Authorization: Bearer $ADMIN_CONSOLE_ACCESS_TOKEN` from callers and
only proxies an allowlist of gateway admin/system routes.
