# Contributing to AI Image Factory

Thank you for helping improve AI Image Factory.

## Before You Start

- Search existing issues and pull requests.
- Keep changes focused on one behavior or architectural boundary.
- Discuss large API, persistence, scheduling, billing, or provider-contract
  changes before implementation.
- Never include real provider credentials, refresh tokens, account identifiers,
  customer prompts, generated private media, or production database exports.

## Development Checks

Run the narrowest relevant tests while iterating. Before requesting review, run:

```bash
cargo fmt --all -- --check
cargo test --workspace --locked
npm ci
npm run typecheck:admin
npm run build:admin
```

Tests that need a real PostgreSQL database or provider account must remain
explicitly opt-in and document their isolation requirements.

## Design Expectations

- Keep public provider-shaped DTOs separate from provider-neutral domain
  contracts.
- Put provider-specific behavior in its adapter.
- Preserve idempotency, lease fencing, durable evidence, and terminal
  reduction for asynchronous work.
- Treat pricing, metering, charging, refunds, and reconciliation as separate
  economic transitions.
- Add an abstraction only when it removes current complexity or matches an
  established repository boundary.
- Keep user-facing console copy complete in English, Simplified Chinese,
  Japanese, and Korean; English is the fallback and default locale.
- Update the relevant architecture or operations document when behavior,
  invariants, activation gates, or release procedures change.

## Pull Requests

Use a conventional commit title when practical. In the pull request:

1. Explain the user or operator problem.
2. Describe the smallest implemented change.
3. List verification commands and results.
4. Call out migrations, compatibility changes, activation gates, and rollback.
5. Include sanitized screenshots for visible UI changes.

By contributing, you agree that your contribution is licensed under the Apache
License 2.0.
