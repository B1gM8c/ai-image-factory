# Task 3 implementation report

## Result

Implemented the additive Grok CLI 1.0.34 video policy and strict receipt path. Existing V1 policy and receipt semantics remain unchanged; V2 calls carry `Exact` argument matching.

## TDD evidence

- RED: `CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p image-provider-grok-cli v2_ -- --nocapture` failed before implementation because `command_spec_video_v2` was absent (and the V2 test helpers had no policy entry point).
- GREEN: `CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p image-provider-grok-cli` passed: 50 unit tests and 0 doc tests.
- Formatting/diff: `cargo fmt --all -- --check` and `git diff --check` passed.
- Secret scan: fixture/source paths contain no credentials or key material; marker scan for private keys, bearer tokens, AWS keys, and `sk-` values passed.

## Capability fixture

`providers/grok-cli-1.0.34-video-capabilities.json` contains only the three registered media tools needed by this feature, with canonical key ordering. `image_gen` came from a same-version 1.0.34 registered-tool session in the repo-spider workspace because the specified ai-image-factory session registered only the two video tools; `image_to_video` and `reference_to_video` came from the specified ai-image-factory session. No absolute session paths or session content were copied.

- observed CLI: `1.0.34 (3736acbc8658)`
- fixture SHA-256: `465b0a6cbe8126cc25b3f099debe37c9d869f3902a3886deb52e42f684e78ab5`
- provider test pins the SHA and the ordered tool names.

## Files

- `providers/grok-cli-1.0.34-video-capabilities.json`
- `crates/provider-grok-cli/src/policy.rs`
- `crates/provider-grok-cli/src/receipt.rs`
- `crates/provider-grok-cli/src/lib.rs`
- `crates/provider-grok-cli/src/tests.rs`

## Behavior

- V2 image-to-video: one exact `image_to_video` call.
- V2 reference-to-video: one exact `reference_to_video` call with deterministic frame/reference order, empty prompt for omitted prompt, normalized voice strings, and no keyframes.
- V2 text-to-video: exactly `image_gen` followed by `image_to_video` in one isolated session.
- V2 retains private HOME/GROK_HOME/workspace, memory/plan/subagent/web-search disables, verbatim dispatch, bounded turns, and streaming JSON.
- V1 video duration/resolution default equivalence remains under `LegacyVideoDefaults`; image prompt normalization remains isolated under `ImagePromptMayNormalize`.

## Risk boundary

The fixture is an immutable capability record, not proof that a live generation succeeds. Task 7 must run the live CLI smoke, especially the empty omitted R2V prompt behavior, before production enablement.

## Commit

Pending final review and commit.
