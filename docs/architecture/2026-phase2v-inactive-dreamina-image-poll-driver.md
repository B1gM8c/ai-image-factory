# Phase 2V: Inactive Dreamina Image Poll Driver

Date: 2026-07-16

Status: implemented and verified with local real-process, cancellation,
artifact, and generic poll-orchestrator tests. Dreamina remains inactive. This
phase performs no external provider call, credential login, quota consumption,
route activation, billing change, or production artifact write.

## Scope

Phase 2V adds the first provider-specific implementation of the Phase 2R poll
driver boundary:

```text
fenced ProviderTaskLease
  -> ProviderPollDriverCall
  -> Dreamina account/profile/auth binding check
  -> digest-pinned query_result process
  -> fresh private download directory
  -> strict bounded receipt
  -> pending | failed | one image file
  -> bounded async artifact stream
  -> epoch-fenced provisional stager
  -> generic orchestrator publication
```

The public OpenAI-compatible image API, active provider list, scheduler,
account selection, pricing, and settlement paths are unchanged.

## Module Boundaries

The implementation keeps provider protocol, process containment, and durable
artifact authority separate:

```text
provider-dreamina-cli
  query argv + strict receipt schema

cli-runtime
  digest-pinned process + process-group cancellation
  fresh-directory identity + bounded async file stream

image-gateway/providers/dreamina_cli
  frozen binding checks + Dreamina outcome classification

image-gateway/provider_tasks/poll
  lease heartbeat + deadline + provisional sink + publication

image-gateway/artifacts
  full image decode + immutable filesystem authority
```

`provider-dreamina-cli` does not depend on the gateway or PostgreSQL.
`cli-runtime` does not depend on a provider or artifact backend. The Dreamina
driver implements the gateway's narrow `ProviderPollDriver` port instead of
pretending to implement unused submit and cancel methods on
`RemoteTaskProvider`.

## Frozen Runtime Binding

`ProviderPollDriverCall` now carries the provider account ID from the claimed
lease in addition to the remote operation and immutable execution context.

Before creating a process, `DreaminaCliPollDriverV1` requires:

- `provider_id = dreamina-cli`;
- the exact provider account ID frozen into the driver binding;
- the exact execution profile ID frozen into the driver binding;
- the exact credential authentication SHA-256 frozen into the binding;
- `command_schema = dreamina-cli.submit.v1`;
- `adapter_revision = dreamina-cli/remote-task/v1`;
- `operation_id = images.generations`;
- `completion_mode = remote_task`; and
- one of the explicitly supported `dreamina-image-*` platform model versions.

A daemon configured for one isolated Dreamina account therefore cannot execute
a lease attached to another account even when both accounts use the same
provider and executable.

The adapter revision changed from the Phase 2P submit-only value
`dreamina-cli/submit/v1` to `dreamina-cli/remote-task/v1`. The canonical submit
payload schema did not change. Dreamina is inactive, so no active durable task
or compatibility contract is migrated by this revision change.

## Query Process Contract

`DreaminaCliQueryPolicyV1` constructs:

```text
dreamina query_result
  --submit_id <durable remote operation ID>
  --download_dir <fresh direct child of the private workspace root>
```

The executable is an absolute, SHA-256-pinned file. The process environment is
cleared by `cli-runtime` and contains only:

```text
HOME=<isolated account home>
TMPDIR=<fresh poll attempt directory>
```

The download directory must be exactly one direct child of the configured
workspace root. The driver creates it with mode `0700`, canonicalizes it, and
binds the open directory descriptor before process launch.

Dropping the poll future drops the CLI runtime future. The existing process
runtime terminates the process group and waits through its bounded termination
contract. The per-attempt `TempDir` is then removed during ordinary
cancellation and error unwinding.

This is graceful-process cleanup, not crash cleanup. `SIGKILL`, host loss, or a
runtime abort can leave an attempt directory. A startup or periodic janitor
with the same ownership and age fencing is still required before activation.

## Receipt Semantics

Stdout must be exactly one JSON object no larger than 64 KiB. Trailing JSON,
missing fields, unknown statuses, invalid IDs, and oversized output fail
closed.

The accepted query states are:

```text
querying -> pending, download directory must remain empty
fail     -> verified terminal failure, download directory must remain empty
success  -> exactly one supported image file must exist
```

The receipt `submit_id` must equal the durable remote operation ID. A mismatch
is protocol drift and never becomes authority.

Process spawn, wait, timeout, capture, and non-zero-exit failures are classified
as retryable transport failures. Invalid command identity, malformed receipt,
unexpected output, or binding drift is non-retryable. The generic orchestrator
records non-terminal contract errors as `uncertain`; only an explicit
`gen_status=fail` with an empty directory becomes a terminal provider failure.

Raw provider failure text is not persisted or logged by this driver.

## Artifact Materialization

`FreshOutputDirectory::seal_single_file_to_async_sink` extends the
provider-neutral output capability with bounded asynchronous streaming:

- the directory must remain owned by the effective user and mode `0700`;
- exactly one directory entry is accepted;
- the entry is opened relative to the held directory descriptor with
  `O_NOFOLLOW`;
- only a regular file with one hard link is accepted;
- the pre-read size must be non-zero and within the configured limit;
- reads use one fixed 64 KiB buffer;
- SHA-256 and byte count are computed during the same pass;
- file identity, timestamps, link count, and size are checked after streaming;
- final directory contents and the current same-name file are checked again;
  and
- a same-name replacement or any second entry fails closed.

The async sink is a lower-level local port. It avoids making `cli-runtime`
depend on `provider-sdk` or the gateway artifact implementation.

The Dreamina bridge captures only the first 12 bytes while forwarding every
chunk. It accepts PNG, JPEG, or WebP signatures. The filesystem stager then
performs the existing full image decode and integrity validation before
creating a staged manifest.

After finalization, the driver compares the CLI-runtime byte count and SHA-256
with the stager manifest. The generic orchestrator separately compares the
driver's completed manifest with the staged authority before publication.
Provider bytes therefore cross two independent integrity comparisons before
durable artifact authority is attached.

Invalid media may reach a provisional stager before signature rejection, but
it cannot be published. Dropping the filesystem stager removes its temporary
file. The durable task becomes uncertain because a provider claimed success
while violating the media contract.

## Performance Bound

One successful image poll performs:

- one provider process;
- one pass over the provider output;
- one fixed 64 KiB read buffer;
- one incremental SHA-256;
- one incremental write into the existing provisional stager;
- at most 12 bytes of media-prefix buffering; and
- no whole-file allocation in the Dreamina driver or CLI runtime.

The filesystem stager still performs its existing full decode pass before
authority publication. That cost is intentional media validation, not an
extra provider adapter copy.

Pending and terminal-failure polls allocate no artifact stager and consume no
materialization semaphore permit.

These are code-path bounds, not production benchmark results. No claim of
SOTA, lowest possible latency, or industry leadership is made without
representative p50/p95/p99 process, provider, filesystem, CPU, memory, and
concurrency measurements.

## Adversarial Verification

Local real-process tests prove:

- pending and terminal failure require an empty download directory;
- a real generated PNG streams in bounded chunks and produces the expected
  byte count and SHA-256;
- mismatched submit IDs, multiple files, unsupported media, oversized files,
  and forged sink manifests fail closed;
- canceling the future terminates the real child process and removes the
  attempt directory;
- unsafe workspace permissions and unbounded artifact limits are rejected;
- PNG, JPEG, and WebP signatures are accepted while MP4 and unknown bytes are
  rejected; and
- the generic `ProviderPollOrchestrator` can execute the concrete Dreamina
  driver, while an account mismatch is rejected before the CLI process starts.

Provider-neutral runtime tests additionally prove bounded async streaming and
same-name replacement detection during an awaited sink write.

No mock claims process cancellation. The cancellation test observes a real
local PID and waits until the operating system reports that it has exited.

No paid provider, external network, production credential, production
artifact root, or Dreamina quota is used.

## Evidence Basis

The official Dreamina installation page, checked on 2026-07-16, publishes the
CLI installer command, requires account login, and states that CLI use is
limited to advanced members:
<https://jimeng.jianying.com/ai-tool/install>.

That public page does not publish a stable machine-readable query receipt or
download artifact schema. The current argv and `gen_status` projection remain
based on the separately inspected CLI contract recorded in Phase 2B and are
protected by an executable digest plus adapter revision. Search results from
third-party pages are not treated as protocol authority.

The official product surface confirms that Dreamina supports both image and
video generation, but it does not prove that the CLI query artifact contract
is identical for those media:
<https://jimeng.jianying.com/>.

These sources justify keeping the provider inactive and the implementation
image-only. They do not prove protocol freshness or production suitability.

## Explicit Limits

Phase 2V does not provide:

- a runnable Dreamina poll service binary or environment contract;
- composition of the Phase 2U database runtime profile into the Phase 2T
  daemon;
- credential secret resolution, login, refresh, or revocation;
- executable installation or an approved production digest;
- crash-left attempt-directory cleanup;
- provider query rate limiting, cooldown, circuit breaking, quota probing, or
  account rotation;
- spend reservation or Dreamina-specific billing evidence;
- production metrics, alert thresholds, or sustained benchmarks;
- a credentialed end-to-end Dreamina smoke test;
- Dreamina activation or model advertisement; or
- Seedance, MP4, duration, frame, codec, audio, or video billing validation.

The existing artifact authority intentionally accepts only PNG, JPEG, and
WebP. Adding MP4 by signature alone would bypass full media validation and
would be an unsafe contract change.

## Next Gate

Phase 2W should compose an inactive, runnable provider poll service without
activating Dreamina:

1. load one Phase 2U profile and derive the exact provider/account claim scope;
2. resolve credentials through a narrow broker that returns an isolated
   account home without exposing secret bytes to provider code;
3. verify the executable digest and construct one provider-specific driver;
4. construct the Phase 2T daemon with durable lane and artifact limits;
5. add startup/shutdown, crash-left workspace janitor, metrics, and redacted
   diagnostics;
6. prove the service with local fake CLI plus real PostgreSQL leases; and
7. keep provider activation, external calls, and video support disabled.

Rate limits, circuit state, quota, spend, and cross-account scheduling should
remain separate policies. They must not be inferred from the execution
concurrency field added in Phase 2U.
