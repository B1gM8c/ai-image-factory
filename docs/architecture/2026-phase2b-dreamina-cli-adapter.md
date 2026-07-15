# Phase 2B: Dreamina CLI Adapter Boundary

Status: accepted for incremental implementation on 2026-07-15.

## Decision

The first non-Codex adapter is the official Dreamina CLI protocol boundary. It
is introduced as `image-provider-dreamina-cli`, but remains inactive in the
gateway until durable remote-task reconciliation, account isolation, artifact
materialization, and billing evidence are complete.

This decision does not treat all JiMeng, Seedream, and Seedance products as one
provider. Four independently versioned protocol families exist:

| Provider boundary | Transport | Authentication | Completion |
| --- | --- | --- | --- |
| Dreamina CLI | local `dreamina` executable | OAuth device flow and isolated CLI home | asynchronous submit and query |
| Ark Seedream | `/api/v3/images/generations` | Ark bearer API key | synchronous or SSE |
| Ark Seedance | `/api/v3/contents/generations/tasks` | Ark bearer API key | asynchronous task API |
| JiMeng Visual OpenAPI | `visual.volcengineapi.com` | Volcengine AK/SK request signing | asynchronous submit and query |

They must not share credential DTOs, request DTOs, retry policies, task status
parsers, or provider identifiers. Product branding is not a protocol contract.

## Verified CLI Contract

The official installer endpoint and CLI help were inspected on 2026-07-15.
The published metadata named release `1.4.12`, while the downloaded arm64
executable reported build identity `2a20fff-dirty`. The executable was unsigned
on macOS. Therefore version text alone is not executable authority.

Production provisioning must:

1. download outside the worker runtime;
2. verify an operator-configured SHA-256 digest;
3. install into an administrator-controlled, non-writable directory;
4. pass an absolute executable path to the adapter; and
5. disable installer, update, login, and shell-configuration commands in the
   worker execution path.

The observed arm64 artifact used during protocol inspection had SHA-256
`8130c6395834ec3100ca03fa4ffcd2155d5001b3e74866b46d83f7c420af0498`.
This is evidence for that inspected artifact, not a permanent allowlist for
future releases.

The supported adapter operations in this phase are:

```text
text2image --prompt ... --model_version ... --ratio ...
           --resolution_type ... --generate_num ... --poll=0

text2video --prompt ... --model_version ... --ratio ...
           --video_resolution ... --duration ... --poll=0

query_result --submit_id ... --download_dir ...
```

Every user value is one argv element. No shell string is constructed. Submit
commands always disable built-in polling so the platform, rather than a local
process, owns durable scheduling and deadlines.

The official image DTO retains `generate_num=1..10`, but the executable policy
accepts only `generate_num=1`. A public batch request is projected into stable
output slots and one provider submission per slot. This preserves the existing
single-output artifact and economic authority instead of hiding several images
behind one remote receipt.

## Crate Boundary

```text
image-provider-contracts       image-cli-runtime
            ^                         ^
            |                         |
            +-- image-provider-dreamina-cli
                              ^
                              |
                     image-gateway composition
```

`image-provider-dreamina-cli` owns only:

- Dreamina model and option enums;
- operation-specific validation;
- argv projection;
- bounded submit-receipt parsing; and
- declared polling, callback, and cancellation capabilities.

It does not own SQL, HTTP routes, public OpenAI DTOs, tenant identity, account
selection, quota, billing, artifact publication, login, or credential refresh.
The gateway may compose it only through the generic provider/runtime ports.

## Process and Receipt Contract

`image-cli-runtime` captures stdout and stderr concurrently into independent
64 KiB buffers while continuously draining both pipes. Retention is bounded,
but draining continues after the bound is reached so a verbose child cannot
deadlock on a full pipe. Leader exit, process-group cleanup, and pipe completion
remain separate events; a descendant that inherits a pipe cannot hide a live
process group behind an apparent output timeout.

Receipt execution declares no artifact output contract. A successful exit is
accepted only when stdout is not truncated and parses as exactly one JSON value
with:

```text
non-empty submit_id
gen_status = querying | success
```

`gen_status=fail` is a rejected submission and exposes only a length-bounded,
control-character-free reason. Unknown status, duplicate/trailing JSON,
non-JSON output, empty identifiers, or oversized stdout/stderr fail closed.
stderr is never interpolated into customer-visible or durable error text.

An accepted submit is not proof that an artifact exists and is not billable
completion evidence.

## Durable Submit Semantics

The current `reserved -> attached` submit intent is insufficient for a remote
operation whose CLI may exit or disconnect after the provider accepted work.
Before activating Dreamina, the durable intent must distinguish:

```text
reserved -> sending -> attached
                    -> rejected
                    -> uncertain
```

- `reserved`: no side effect has been attempted.
- `sending`: the side-effecting process may have reached the provider.
- `attached`: a validated stable `submit_id` is durably bound.
- `rejected`: evidence proves no accepted remote task for this attempt.
- `uncertain`: the process may have submitted work, but no stable identifier
  was recovered.

Automatic submit retry is forbidden from `sending` or `uncertain` unless the
official protocol gains a provider-enforced idempotency key or a reconciliation
operation can prove absence. A customer idempotency key deduplicates platform
requests; it does not make the upstream CLI idempotent.

## Credentials and Accounts

The adapter receives an explicit, account-scoped CLI home from a credential
broker. It must not inherit the service user's `HOME`, `PATH`, proxy variables,
or interactive login state. A worker clears its environment and adds only the
minimum allowlist required by a provisioned account session.

Account selection, concurrency, cooldown, and spend caps belong to the
scheduler/composition layer. They are keyed by an opaque account identifier,
never by a credential value. Logs, task records, metrics labels, and billing
receipts may not contain access tokens, cookies, authorization headers, CLI
session files, or temporary download URLs.

The first activation is single-account and polling-only. Multi-account routing
requires independent homes, filesystem ownership, concurrency leases, and
account-level circuit breakers.

## Polling and Cancellation

The inspected CLI exposes `query_result`, but no stable callback verifier or
provider-enforced cancellation operation was established. Phase 2B therefore
declares:

```text
polling: supported
callbacks: unsupported
cancellation: unsupported
```

A local customer cancellation may stop future polling or artifact delivery,
but it must not claim that the remote provider stopped work. Capacity and spend
accounting remain conservative until a terminal remote observation or an
absolute orphan deadline resolves the task according to policy.

Poll scheduling uses the existing PostgreSQL fenced lease. Waiting remote
tasks do not hold a local executor lease. Backoff is bounded and jittered, and
every task has an absolute provider deadline plus an orphan reconciler. A poll
response is an append-only observation; it cannot directly grant canonical
terminal or billing authority.

## Artifact Authority

`query_result --download_dir` writes provider-selected files into a directory.
That directory is untrusted staging, not durable artifact authority. Activation
requires a materializer that:

1. creates a fresh per-attempt directory with a quota;
2. records its pre-query contents;
3. rejects symlinks, hard links, devices, nested paths, and unexpected files;
4. accepts exactly the expected output cardinality and media type;
5. streams each file through fixed-size buffers while hashing and enforcing a
   byte limit;
6. publishes immutable object storage before writing the manifest; and
7. binds the manifest through the live poll fence before terminal reduction.

Temporary provider URLs are never persisted. If a future Ark adapter downloads
remote URLs directly, it additionally needs DNS/IP allowlisting, redirect
revalidation, private-network denial, response-size limits, and content-type
verification to close SSRF and resource-amplification paths.

## Billing Evidence

Submission acceptance and provider task success are not sufficient billing
evidence. The economic kernel may charge only after canonical terminal
reduction references a verified artifact manifest and an immutable usage fact.

Image usage records the validated output count and selected model/options.
Video usage records provider-confirmed or artifact-verified duration and output
count; requested duration alone is not proof of delivered seconds. Pricing is a
versioned platform policy selected before admission. Provider-native credit
responses are operational signals, not the customer ledger.

## Activation Gates

Dreamina remains absent from `active_providers()` and `/v1/models` until all of
the following are green:

1. exact CLI digest provisioning and isolated account home;
2. `reserved/sending/attached/rejected/uncertain` migration and crash tests;
3. fenced poll daemon, absolute deadline, and orphan reconciliation;
4. untrusted download-directory materializer and immutable artifact manifest;
5. image-count and video-duration billing evidence;
6. account concurrency, spend cap, cooldown, and circuit breaker;
7. fault injection for exit-before-receipt, accepted-but-lost receipt,
   duplicate poll, malformed JSON, oversized output, process descendants,
   disk exhaustion, and worker restart; and
8. a credentialed, explicitly approved smoke test that creates one image,
   queries it to completion, verifies its digest/MIME, and proves exactly one
   economic resolution.

Until then, tests use scripted executables and no paid upstream request.

## Rejected Alternatives

- Do not infer protocol compatibility from the words JiMeng, Dreamina,
  Seedream, or Seedance.
- Do not activate roadmap metadata before request validation and execution are
  reachable end to end.
- Do not invoke the official installer or auto-update from a worker.
- Do not retry an ambiguous submit.
- Do not persist arbitrary upstream JSON for future convenience.
- Do not expose callback or cancellation flags that have no verified official
  implementation.
- Do not load a generated video into one in-memory `Vec<u8>`.
- Do not let adapters mutate queue, ledger, or terminal state directly.

## Primary References

- Dreamina CLI installation: https://jimeng.jianying.com/ai-tool/install
- Ark Seedream image generation:
  https://www.volcengine.com/docs/82379/1541523
- Ark Seedance create-task API:
  https://www.volcengine.com/docs/82379/1520757
- Ark Seedance query-task API:
  https://www.volcengine.com/docs/82379/1521309
- Ark Seedance cancel/delete API:
  https://www.volcengine.com/docs/82379/1521720
- JiMeng Visual image API:
  https://www.volcengine.com/docs/85621/2275082
- JiMeng Visual video API:
  https://www.volcengine.com/docs/85621/1777001
- Volcengine request signing:
  https://www.volcengine.com/docs/6369/67270?lang=zh
