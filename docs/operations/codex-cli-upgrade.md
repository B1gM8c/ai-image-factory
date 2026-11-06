# Codex CLI preview upgrade and native image formats

The selected runtime is **0.168.0-alpha.7**, an explicit preview pin. Do not
substitute a moving `alpha` or `latest` tag. Recovery rehearsal installs that
exact official package in its disposable runner and records wrapper/package
integrity and the native executable SHA-256. Production activation is a
separate, owner-authorized maintenance step.

## Verified capability boundary (2026-11-06)

Official tag `rust-v0.168.0-alpha.7` keeps the built-in image tool's model as
`gpt-image-2`. Its `gif` argument is available only when the under-development
`image_generation_animation` feature is enabled. It exposes neither an image
model selector nor native SVG/complexity/markup parameters.

Two explicitly authorized, isolated subscription CLI test turns established:

- Initialize, configuration/account reads and thread creation work with the
  preview CLI. No API key or Blog generation endpoint was used.
- Enabling animation caused HTTP 400 before an image-tool invocation: the
  server rejected the reserved `image_gen.imagegen` schema. Image calls and
  generated files were both zero. Do not retry this as an accepted generation.
- With animation disabled, the SVG capability check completed and reported
  that native SVG output is unavailable. It made no image-tool call and wrote
  no generated file. Hand-authoring SVG with a coding tool is a different
  operation and does not establish GPT Image 2.6 support.

Consequently, keep animation **disabled** and keep GIF/SVG/2.6 generation
capabilities and prices unavailable. New format validation/preservation code
does not authorize a new model, request parameter or paid specification. Do
not add unsupported fields only to prompts or relabel the fixed model.

The official Image API documents GPT Image 2.6 and native GIF/SVG features;
that does not prove subscription CLI access. A future Image API integration
would require its own authentication, cost and execution contract.

## Production preparation and activation

1. Complete the normal source checks, CI and independent review. Notify the
   owner before production maintenance; preparing this change is not approval
   to activate a preview runtime immediately.
2. Stage the official exact-version native executable under the root-owned,
   nonwritable version directory
   `/opt/ai-image-factory/provider-tools/codex/0.168.0-alpha.7/`. Verify package
   integrity, target architecture, executable SHA-256 and `--version` as the
   service UID. Retain the prior executable for frozen executions and rollback.
   Do not overwrite `/usr/local/bin/codex` or replace a file at a frozen path.
3. Inventory active/recoverable Codex executions and their immutable executable
   bindings. Drain/quiesce through the normal maintenance procedure. Never
   rewrite an old execution's executable path/SHA to match the new runtime.
4. In the separately authorized window, configure the managed-account CLI and
   each **Codex** executor profile to the same verified native executable:
   `GATEWAY_MANAGED_CODEX_EXECUTABLE` and profile-specific
   `EXECUTOR_PROVIDER_EXECUTABLE`. Do not put the latter in shared Grok config.
   Activate application changes through normal owner Check/Apply if needed.
5. Verify service UID startup, real process/executable identity, health,
   readiness and queue/recovery state before reopening admission. Do not create
   a paid canary or replay customer work. Readiness alone is not proof of a new
   image generation or of animation availability.

## Sources

- [Official image tool source at the exact preview tag](https://github.com/openai/codex/blob/rust-v0.168.0-alpha.7/codex-rs/ext/image-generation/src/tool.rs)
- [Preview feature gate](https://github.com/openai/codex/blob/rust-v0.168.0-alpha.7/codex-rs/features/src/lib.rs)
- [Official Image API guide](https://developers.openai.com/api/docs/guides/image-generation)
