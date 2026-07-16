# AI Image Factory

AI Image Factory is a monorepo for an OpenAI-compatible image API platform.

The first active backend is the existing native Codex CLI path for `gpt-image-2`.
The platform layout now leaves explicit room for additional providers such as
Dreamina CLI, Volcengine Ark, and Grok CLI without coupling their native
protocols to the public API.

## Workspace

```text
apps/
  admin-console/      Next.js + React + shadcn-style operations console
crates/
  cli-runtime/            Provider-neutral Unix process and artifact runtime
  image-gateway/          HTTP composition, PostgreSQL workflows, and service binaries
  provider-contracts/     Immutable media/provider/job contracts and roadmap
  provider-dreamina-cli/  Inactive official Dreamina CLI protocol adapter
  provider-sdk/           Inline and remote provider execution ports
  provider-test-support/  Dev-only provider conformance harness
  scheduler-policy/       Provider-neutral weighted scheduling policy
docs/
  architecture/           Decisions, activation gates, and target boundaries
```

The authoritative target design is
[`docs/architecture/2026-ai-image-factory-target-architecture.md`](docs/architecture/2026-ai-image-factory-target-architecture.md).
The current CLI execution boundary and activation gates are documented in
[`docs/architecture/2026-phase2a-provider-runtime-boundaries.md`](docs/architecture/2026-phase2a-provider-runtime-boundaries.md).
The inactive Dreamina CLI adapter and its production gates are documented in
[`docs/architecture/2026-phase2b-dreamina-cli-adapter.md`](docs/architecture/2026-phase2b-dreamina-cli-adapter.md).
The remote-submit deadline quarantine is documented in
[`docs/architecture/2026-phase2f-provider-submit-deadline-quarantine.md`](docs/architecture/2026-phase2f-provider-submit-deadline-quarantine.md).
Its independent capacity reconciliation and strong-evidence release boundary is documented in
[`docs/architecture/2026-phase2g-provider-capacity-reconciliation.md`](docs/architecture/2026-phase2g-provider-capacity-reconciliation.md).
Atomic remote artifact evidence and canonical resolution are documented in
[`docs/architecture/2026-phase2h-atomic-provider-artifact-resolution.md`](docs/architecture/2026-phase2h-atomic-provider-artifact-resolution.md).
Exact submit-recovery command replay and bounded claims are documented in
[`docs/architecture/2026-phase2i-replayable-provider-submit-recovery.md`](docs/architecture/2026-phase2i-replayable-provider-submit-recovery.md).
Attached remote-task deadlines, quarantine authority, and committed artifact recovery are documented in
[`docs/architecture/2026-phase2j-provider-remote-task-deadline.md`](docs/architecture/2026-phase2j-provider-remote-task-deadline.md).
Immutable operation descriptors, command identity, submit idempotency, and execution binding are documented in
[`docs/architecture/2026-phase2k-immutable-provider-operation-binding.md`](docs/architecture/2026-phase2k-immutable-provider-operation-binding.md).
Atomic provider-submit dispatch and its single orchestration boundary are documented in
[`docs/architecture/2026-phase2l-atomic-provider-submit-orchestrator.md`](docs/architecture/2026-phase2l-atomic-provider-submit-orchestrator.md).
Durable local submit evidence, receipt-first recovery, and its remaining helper gates are documented in
[`docs/architecture/2026-phase2m-durable-provider-submit-journal.md`](docs/architecture/2026-phase2m-durable-provider-submit-journal.md).
The inactive gated CLI process protocol, crash evidence, and containment limits are documented in
[`docs/architecture/2026-phase2n-gated-cli-submit-runner.md`](docs/architecture/2026-phase2n-gated-cli-submit-runner.md).
The inactive gated submit composition, static driver boundary, and crash-window recovery are documented in
[`docs/architecture/2026-phase2o-gated-submit-orchestration.md`](docs/architecture/2026-phase2o-gated-submit-orchestration.md).
The inactive Dreamina canonical submit codec and its gated runtime binding are documented in
[`docs/architecture/2026-phase2p-dreamina-gated-submit-codec.md`](docs/architecture/2026-phase2p-dreamina-gated-submit-codec.md).
The provider-neutral fresh CLI output-directory boundary is documented in
[`docs/architecture/2026-phase2q-fresh-cli-output-directory.md`](docs/architecture/2026-phase2q-fresh-cli-output-directory.md).
The provider-neutral fenced poll orchestrator, lazy materialization boundary,
and committed-authority recovery are documented in
[`docs/architecture/2026-phase2r-provider-poll-orchestrator.md`](docs/architecture/2026-phase2r-provider-poll-orchestrator.md).
The epoch-fenced streaming filesystem stager, immutable publication, and
pre-authority crash replay are documented in
[`docs/architecture/2026-phase2s-epoch-staged-provider-artifacts.md`](docs/architecture/2026-phase2s-epoch-staged-provider-artifacts.md).
The provider-neutral fixed-lane poll daemon, jittered pacing, and bounded
shutdown drain are documented in
[`docs/architecture/2026-phase2t-provider-poll-daemon.md`](docs/architecture/2026-phase2t-provider-poll-daemon.md).
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
