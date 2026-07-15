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
  image-gateway/      Rust gateway plus workerd/executord/reducerd/reconcilerd binaries
  provider-contracts/ Shared media/provider/job contracts and roadmap slots
  scheduler-policy/  Provider-neutral weighted scheduling policy
docs/
  architecture/       Upgrade notes and platform boundaries
```

The authoritative target design is
[`docs/architecture/2026-ai-image-factory-target-architecture.md`](docs/architecture/2026-ai-image-factory-target-architecture.md).
The current CLI execution boundary and activation gates are documented in
[`docs/architecture/2026-phase1f-executor-runtime.md`](docs/architecture/2026-phase1f-executor-runtime.md).
Database-bound provider profiles and durable capacity are documented in
[`docs/architecture/2026-phase1g-execution-binding-capacity.md`](docs/architecture/2026-phase1g-execution-binding-capacity.md).

## Common Commands

```bash
cargo test --workspace
cargo run -p gpt-image-2-gateway
cargo run -p gpt-image-2-gateway --bin workerd
cargo run -p gpt-image-2-gateway --bin executord
cargo run -p gpt-image-2-gateway --bin reducerd
cargo run -p gpt-image-2-gateway --bin reconcilerd
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
