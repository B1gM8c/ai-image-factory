# Phase 2P: Inactive Dreamina Gated Submit Codec

Date: 2026-07-16

Status: implemented and unit-verified as inactive composition. Dreamina remains
absent from active provider metadata, `/v1/models`, public routing, credential
loading, and daemon startup. No installer, login, quota, or external generation
call is executed by this phase.

## Decision

The first provider-specific Phase 2O composition is the official Dreamina CLI
submit protocol. It is split across two one-way layers:

```text
image-provider-dreamina-cli
  canonical submit payload
  official argv and environment projection
  strict bounded receipt parser
             |
             v
gpt-image-2-gateway/providers/dreamina_cli
  frozen runtime binding checks
  CommandSpec -> GatedCliCommand conversion
  receipt -> PendingOperation conversion
```

The provider crate does not depend on the gateway. The gateway is the
composition root and depends on the provider crate. This avoids a cycle and
keeps SQL, scheduling, journal authority, account selection, and process
recovery outside the protocol adapter.

Platform operation IDs and model aliases exist only in the gateway composition
layer. The provider crate knows only the official CLI request, argv, receipt,
and canonical command contracts.

## Unique Execution Path

`DreaminaCliPolicyV1` no longer implements `ReceiptCliPolicy`. Therefore the
provider crate cannot be passed to `CliRuntime::run_receipt` to launch a
side-effecting submit directly.

The policy exports only:

- a digest-pinned `CommandSpec` projection; and
- the configured executable SHA-256 needed by the gated process protocol.

The only gateway composition is:

```text
ProviderSubmitOrchestrator
  -> GatedCliSubmitDriver<DreaminaCliSubmitCodecV1>
  -> remote-submit-runner
  -> digest-pinned dreamina executable
```

This phase adds no second submit daemon, direct provider future, retry loop,
message broker, runtime registry, or alternative process supervisor.

## Canonical Command

`DreaminaSubmitPayloadV1` implements the provider SDK
`CanonicalCommandPayload` contract with:

```text
schema: dreamina-cli.submit.v1
adapter revision: dreamina-cli/submit/v1
```

The canonical JSON uses the inspected official option names:

- `text2image`: `prompt`, `model_version`, `ratio`,
  `resolution_type`, `generate_num`, `poll`;
- `text2video`: `prompt`, `model_version`, `ratio`, `duration`,
  `video_resolution`, `poll`.

The parser:

- accepts exactly schema version 1;
- requires `poll=0`;
- rejects unknown fields, trailing JSON, unknown model/ratio/resolution values,
  invalid model-resolution combinations, and invalid duration;
- rejects hidden provider batches where `generate_num != 1`; and
- re-runs typed request validation after decoding durable bytes.

The source admission command SHA-256 remains a separate SDK identity field.
The provider command digest also binds the canonical payload and output slot.

## Runtime Binding

`DreaminaCliRuntimeBindingV1` freezes three values into the codec instance:

- execution profile ID;
- provider account ID; and
- credential authentication SHA-256.

Before command projection, the codec compares these values with the
PostgreSQL-frozen submit intent and execution context. It also checks provider
ID, command schema, adapter revision, output slot, operation ID, and the
platform model-to-official-model mapping.

This prevents an orchestrator instance prepared for one isolated Dreamina home
from silently executing a submission frozen for another account or profile.
Credential bytes, cookies, and session files are never passed to the codec or
persisted in the process journal.

## Executable And Environment

`DreaminaCliPolicyV1::new` now accepts an absolute executable path plus the
operator-configured SHA-256 and verifies them together. It does not accept an
unqualified `VerifiedExecutable` that may have been created without a digest.

The projected process environment is cleared by the runner and contains only:

- `HOME=<isolated account home>`;
- `TMPDIR=<per-execution workspace>`.

Arguments remain separate `argv` values. Shell command strings are not built.
The generic gated command re-verifies the executable digest before durable
process preparation, and the runner verifies it again before exec.

## Receipt Semantics

A successful, untruncated CLI exit is accepted only when stdout is exactly one
bounded JSON object whose:

- `gen_status` is `querying` or `success`; and
- `submit_id` is a valid durable opaque identifier.

The codec converts that identifier to:

```text
provider_id = dreamina-cli
submission_id = frozen platform submission UUID
operation_id = Dreamina submit_id
next_poll_after_ms = 1000
```

Malformed, missing, oversized, failed, or unknown-status receipts remain
`UnknownRemoteEffect` with `RetryDirective::Never`. Although the parser can
recognize `gen_status=fail`, this phase does not claim that every such response
proves the provider created no remote work. Conservative ambiguity prevents an
unsafe automatic resubmit until that semantic is established from stronger
official evidence.

## Current Official Evidence

The official Dreamina install page, checked on 2026-07-16, publishes:

```text
curl -s https://jimeng.jianying.com/cli | bash
```

and states that CLI use is limited to advanced members:
<https://jimeng.jianying.com/ai-tool/install>.

The worker does not run that installer. Exact submit/query argv remains frozen
from the separately inspected CLI contract recorded in Phase 2B. A future
version upgrade requires an explicit digest and protocol review; brand or
version text is not executable authority.

## Verification

Provider protocol tests prove:

- stable canonical image submit bytes;
- strict version, polling, unknown-field, trailing-JSON, official-option, and
  single-output rejection;
- exact image/video argv projection without shell interpolation;
- digest-pinned executable policy construction;
- isolated `HOME` and `TMPDIR` projection; and
- bounded strict receipt parsing.

Gateway codec tests prove:

- pinned policy conversion into a generic gated command;
- accepted receipt binding to the frozen platform submission;
- fixed polling delay projection;
- malformed and provider-failed receipts remain unknown-effect and
  non-retryable; and
- nil or malformed runtime identities fail before execution; and
- credential authentication digests remain redacted from runtime binding debug
  output.

The Phase 2O real PostgreSQL tests continue to prove unique dispatch, released
gate recovery, durable receipt replay, and pre-release deadline behavior for
the generic driver. This phase does not claim a credentialed Dreamina
end-to-end smoke test.

## Remaining Activation Gates

1. Add a provisioner that binds the inspected Dreamina executable digest,
   isolated account home, credential authentication digest, and execution
   profile without exposing credential bytes.
2. Add the Dreamina poll codec and untrusted download-directory materializer
   with streaming limits and immutable artifact authority.
3. Add provider/account daemon composition, cooldown, spend limits, and
   production-scale mixed-account benchmarks.
4. Add Linux cgroup v2 containment and define the macOS production supervisor
   contract.
5. Run an explicitly approved credentialed smoke test only after every prior
   gate is green.

Until then, Dreamina remains planned/inactive and the public Codex image
behavior is unchanged.
