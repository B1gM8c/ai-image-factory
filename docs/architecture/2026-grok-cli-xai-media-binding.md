# Grok CLI to xAI media API binding

> Status: xAI image and video contracts, Grok projection, schema-scoped
> handoff, digest-bound input staging, separate image/video runtime profiles,
> fenced execution, bounded image/MP4 artifact publication, Media Economics V3,
> and the default-off public asynchronous video routes are implemented
>
> Verified: 2026-07-18 against the official xAI documentation and OpenAPI,
> the official `xai-org/grok-build` source, installed Grok CLI `0.2.102`, fake
> crash/replay tests, PostgreSQL migration/profile/launch-context tests, and one
> explicitly approved real 6-second 480p image-to-video invocation through the
> durable supervisor. It produced a validated 431,280-byte MP4 with SHA-256
> `0fe33c5cc6ca999409f9a370dbdf6138160817b3dbe2f3bc560d7fc50e65e5d9`.

## Decision

The factory exposes xAI-shaped HTTP contracts and treats Grok Build as one
execution binding. API compatibility and execution compatibility are separate:

- the xAI facade owns HTTP field names, status codes, synchronous image
  responses, asynchronous video jobs, and response projection;
- `provider-grok-cli` owns only the subset the installed CLI can execute
  without silently changing the admitted request;
- the scheduler, launch authority, artifact store, and billing ledger remain
  provider-neutral;
- unsupported xAI options fail before scheduling. They are never dropped or
  approximated implicitly.

The image model catalog remains separately gated. Video routing is controlled by
`GATEWAY_ENABLE_XAI_VIDEO_API`, defaults to `false`, and does not advertise an
unrelated image model through `active_providers()`.

## Primary sources

- [xAI image REST API](https://docs.x.ai/developers/rest-api-reference/inference/images)
- [xAI image generation guide](https://docs.x.ai/developers/model-capabilities/images/generation)
- [xAI video REST API](https://docs.x.ai/developers/rest-api-reference/inference/videos)
- [xAI video generation guide](https://docs.x.ai/developers/model-capabilities/video/generation)
- [xAI image-to-video guide](https://docs.x.ai/developers/model-capabilities/video/image-to-video)
- [xAI reference-to-video guide](https://docs.x.ai/developers/model-capabilities/video/reference-to-video)
- [xAI Grok Imagine Video 1.5 model](https://docs.x.ai/developers/models/grok-imagine-video-1.5)
- [xAI OpenAPI](https://api.x.ai/api-docs/openapi.json)
- [official Grok Build source](https://github.com/xai-org/grok-build)

## Capability matrix

| xAI surface | Official behavior | Grok CLI binding | Factory decision |
| --- | --- | --- | --- |
| `POST /v1/images/generations` | synchronous; official request and response schemas include model, count, ratio, resolution, response format, storage, user attribution, file output, and usage | `image_gen`; one local JPEG; effective `1k`; no upstream URL or Files API handle is exposed | retain the full official DTO; currently admit only `n=1`, omitted/`1k` resolution, explicit `b64_json`, and no `storage_options` |
| `POST /v1/images/edits` | synchronous image edit | `image_edit`; 1-3 source images; quality model; one `1k` image | keep inactive until the full official edit DTO, typed source hash, sealed input staging, and cleanup path are implemented |
| `POST /v1/videos/generations` text-only | asynchronous | no direct CLI tool | reject; do not hide an image generation plus image-to-video double charge |
| `POST /v1/videos/generations` with one image | asynchronous | `image_to_video`; 6 or 10 seconds; `480p` or `720p` | support as a platform async job even though the CLI polls internally |
| `POST /v1/videos/generations` with reference images | asynchronous | `reference_to_video`; 2-7 images; five ratios; 6 or 10 seconds; `480p` or `720p` | support as a platform async job |
| `POST /v1/videos/edits` | asynchronous | no CLI tool | reject |
| `POST /v1/videos/extensions` | asynchronous | no CLI tool | reject |
| `GET /v1/videos/{request_id}` | returns provider task progress and final URL | CLI hides the provider request ID and returns after polling | expose the factory job ID and factory state, not a fabricated xAI provider ID |

Image `2k`, batches, `response_format=url`, `storage_options`, video edits,
video extensions, and text-only video are not part of the CLI binding. These
official fields remain in `api-contracts`; the projector returns a field-specific
unsupported error before admission instead of dropping them. A future direct
xAI API adapter can add them without changing the public facade or scheduler
contracts.

The versioned request DTO rejects unknown fields. This is deliberate for a paid
execution boundary: a newly added official field must first be modeled and
classified by each binding, rather than being accepted and silently discarded.
Response DTOs remain forward-compatible when decoding additional fields.

The official OpenAPI schema defines `n=1`, `aspect_ratio=auto`,
`resolution=1k`, and `response_format=url`. Admission resolves those defaults
before hashing, so omitted and explicitly defaulted requests have one canonical
identity. The Grok binding then rejects values outside its capability subset.

## Fixed model binding

The official CLI source currently binds tools as follows:

| Tool | Model sent by the CLI |
| --- | --- |
| `image_gen` | `grok-imagine-image-quality` by default; generation can explicitly bind `grok-imagine-image` through the CLI model override |
| `image_edit` | `grok-imagine-image-quality` |
| `image_to_video` | `grok-imagine-video-1.5-preview`, an official alias of `grok-imagine-video-1.5` |
| `reference_to_video` | `grok-imagine-video` |

These model identities are part of the canonical provider command. The xAI
projector accepts the official 1.5 model name and its preview/dated aliases for
image-to-video, then records the concrete CLI execution identity. The current
image-generation projector activates both `grok-imagine-image` and
`grok-imagine-image-quality`, preserving the selected model through admission,
the durable command, and execution. Image editing remains quality-only. A
request cannot claim an unrelated model while the CLI executes another.

The adapter contract revision remains pinned to the validated `0.2.102`
contract baseline. Runtime profiles additionally bind the executable digest;
the base-generation path was exercised end to end with installed Grok CLI
`0.2.106`. A CLI upgrade still requires capability revalidation before
activation.

## Runtime boundary

Each admitted output receives a unique private attempt workspace and a unique
lowercase UUID session ID. The launch command:

- canonicalizes and hashes the absolute executable once, uses no shell, and
  makes the child revalidate that digest before launch; production additionally
  requires the expected provider and helper digests to come from a trusted
  deployment manifest rather than from the selected files themselves;
- clears the inherited environment in `cli-runtime`;
- uses a clean private `HOME` so user skills, plugins, MCP servers, and rules do
  not enter the agent context;
- uses a separate private `GROK_HOME` for the authenticated account and session
  receipt;
- enables exactly one media tool and disables memory, planning, subagents, and
  web search;
- sends the dispatch prompt through stdin, never argv;
- applies a hard wall timeout and process-group termination;
- leaves actual process launch under the existing executor authority;
- carries a provider-neutral `RunnerLaunchBinding`, and the helper verifies the
  exact journal spec plus owner/epoch launch marker before it writes process
  evidence or starts the CLI.

`LaunchCommitted` recovery attaches directly to durable process/output
evidence. It does not reload mutable launch context or overwrite the isolated
credential copy.

`workerd` claims executor-handoff work by both economics contract and exact
command schema. A Grok worker therefore cannot lease an older Codex item at the
head of the same durable queue. `executord` loads one database-bound profile,
verifies its provider, command schema, operation descriptor digest, completion
mode, idempotency mode, adapter revision, credential identity, and resource
policy, then constructs exactly one matching supervisor. Public video routing is
an independent startup gate and never changes executor processing for already
accepted work.

The account-level scheduler must cap concurrent Grok sessions. Shared OAuth
credential refresh is an account resource even when attempt workspaces and
session IDs are independent. These process controls are sufficient for local
adapter verification, not production activation of an agentic CLI. Production
requires the isolation boundary mandated by the target architecture (for
example gVisor, Kata Containers, or a microVM), explicit network egress policy,
and CPU, memory, process, file, and wall-time limits.

## Command and result proof

The provider command is immutable and output-bound through
`SingleOutputCommand`. Its canonical JSON includes fixed CLI behavior such as
`n=1`, effective `resolution=1k`, the concrete tool model, staged filenames, and
staged content hashes. The image-generation payload persists the typed official
xAI source command as well as `source_command_sha256`. Admission reparses that
source, recomputes its hash, reprojects it into the Grok request, and requires
an exact field-for-field match before the work item can be attached. Supported
facade-only fields such as `user` therefore survive restart; unsupported
official fields never reach the queue. Video data URLs are decoded and sealed
before attachment; durable command JSON stores only staged SHA-256 references,
while the original source-command hash remains the idempotency binding.

A CLI exit code alone is insufficient. A successful receipt requires all of:

1. bounded streaming JSON ending in exactly one `end` event;
2. the expected session ID and a non-empty request ID;
3. exactly one call to the one admitted media tool;
4. tool arguments exactly equal to the admitted JSON;
5. a tool result whose path, filename, and media folder equal the deterministic
   session artifact path;
6. an exact deterministic artifact path in the tool result.

This receipt is evidence, not artifact authority. The supervisor opens history
and artifact files relative to the bound private `provider-home` fd, walks every
component with `openat` and `O_NOFOLLOW`, rejects aliases and writable files,
checks stable metadata before and after the bounded read, then hands sealed
bytes to the provider-neutral artifact sink. Image decoders validate image
bytes; a bounded ISO BMFF parser requires a leading `ftyp` and non-empty `moov`
and `mdat` boxes before accepting `video/mp4`. Agent token usage is observability
only; it is not authoritative media billing. The receipt's
`headless_request_id` and `effective_tool_prompt` are CLI evidence fields; they
must never be exposed as an xAI video task ID or xAI `revised_prompt`.

After the artifact is sealed, the helper removes the private Grok home, runtime
home, staged inputs, and generated CLI media path. The artifact-store object
remains platform recovery authority until the platform retention lifecycle
deletes it. The durable runner-journal `output.bin` remains a separate
execution-local recovery boundary; deleting it safely requires terminal
database convergence and exclusive runner-lock ownership and is not performed
by platform artifact retention.

## Billing and idempotency

- image generation and edits reserve and settle one output;
- video admission models exactly one output, the admitted duration as billing
  units, and the duration as scheduler cost;
- Media Economics V3 persists `output_count=1` independently from
  `billable_units=duration`, and quota, immutable metering, rating, account
  capture, and double-entry ledger posting all use `video_second` / `second`;
- active video prices require a positive success price. Missing pricing returns
  `PricingUnavailable`; migration `0033` does not seed a zero wildcard video
  price;
- retries remain submission-bound because the CLI has no provider idempotency
  token;
- an ambiguous timeout must not trigger an automatic paid retry until
  reconciliation proves no committed artifact exists;
- terminal video inputs are reconciliation cleanup candidates; uncertain work
  retains its sealed inputs and evidence;
- successful status projection returns the factory Files content URL. CLI-local
  input/output paths are removed, while the verified customer artifact remains
  under the platform retention lifecycle;
- response projection must never cause a second provider invocation.

## Code ownership

```text
crates/
  api-contracts/
    src/xai/
      images.rs          # official xAI image DTO and canonical source command
      videos.rs          # official xAI async video DTO and source command
  provider-grok-cli/
    src/
      capabilities.rs  # exact supported operations
      request.rs       # legal CLI subset and staged inputs
      command.rs       # canonical command identity and strict decoder
      policy.rs        # isolated CommandSpec and deterministic session paths
      receipt.rs       # stdout/history/artifact proof
      xai.rs             # explicit xAI image compatibility projector
      xai_video.rs       # explicit xAI video compatibility projector
      tests.rs         # contract and adversarial cases
  image-gateway/
    src/admission/
      xai_images.rs      # official image request -> immutable admission plan
      xai_videos.rs      # one output vs duration billing/schedule plan
    src/executor/
      grok_request.rs     # lease/context/model projection
      grok_supervisor.rs  # fenced helper, receipt, sealed image bytes
      grok_supervisor/
        live_smoke.rs     # opt-in real CLI test; never runs in normal CI
    src/bin/
      grok-runner.rs      # detached durable helper entrypoint
```

The provider crate depends inward on `api-contracts`, `provider-contracts`,
`provider-sdk`, and `cli-runtime`. It does not depend on the gateway, database,
or scheduler. The xAI HTTP contract does not depend on Grok.

## Runtime operations

The database identity graph is provisioned explicitly; provisioning never
activates a public route. Image and video profiles use distinct operation
bindings:

```bash
EXECUTOR_CREDENTIAL_HOME=/private/grok-credential-home \
EXECUTOR_PROFILE_KEY=grok-image-v1 \
EXECUTOR_CREDENTIAL_POOL_KEY=grok-membership-pool \
EXECUTOR_PROVIDER_ACCOUNT_KEY=grok-account-1 \
EXECUTOR_CREDENTIAL_REF=mounted.grok.account-1.1 \
EXECUTOR_CREDENTIAL_REVISION=1 \
EXECUTOR_MAX_CONCURRENCY=1 \
cargo run -p gpt-image-2-gateway --bin factoryctl -- provision-grok-profile
```

Use `provision-grok-video-profile` with a distinct
`EXECUTOR_PROFILE_KEY`. Publish an explicit positive `video_second` price for
`xai-videos-v1` / `video_generation` / `grok-cli`, configure tenant credit and
quota, then enable the public routes with
`GATEWAY_ENABLE_XAI_VIDEO_API=true`. An invalid boolean value fails startup.

`workerd` uses `WORKER_EXECUTION_MODE=executor-handoff` and the exact profile
key. `executord` accepts provider-neutral `EXECUTOR_PROVIDER_EXECUTABLE` and
`EXECUTOR_CREDENTIAL_HOME`; the existing Codex-specific names and the new
`EXECUTOR_GROK_EXECUTABLE` / `EXECUTOR_GROK_CREDENTIAL_HOME` remain explicit
fallbacks. It defaults `EXECUTOR_PROCESS_STARTUP_GRACE_MS` to 60 seconds so a
signed provider CLI cold start is not misclassified as a missing durable
process.

The real image smoke is ignored by default and requires all three variables:

```bash
GROK_SMOKE_CREDENTIAL_HOME="$HOME/.grok" \
GROK_SMOKE_EXECUTABLE="$HOME/.grok/bin/grok" \
GROK_SMOKE_HELPER_EXECUTABLE="$PWD/target/debug/grok-runner" \
cargo test -p gpt-image-2-gateway --lib \
  executor::grok_supervisor::live_smoke::xai_generation_runs_through_the_real_durable_grok_supervisor \
  -- --ignored --exact --nocapture
```

That test starts from `XaiImageAdmissionPlan`, enters
`JournaledDurableRunner::start_or_attach(AllowLaunch)`, spawns the detached
helper, invokes the real CLI, verifies the receipt and artifact, publishes via
the executor artifact sink, and replays the terminal result without a second
launch. It consumes one membership image allowance.

The corresponding video smoke uses the same variables and durable path:

```bash
cargo test -p gpt-image-2-gateway --lib \
  executor::grok_supervisor::live_smoke::xai_image_to_video_runs_through_the_real_durable_grok_supervisor \
  -- --ignored --exact --nocapture
```

It stages a digest-bound JPEG, invokes `image_to_video` once at 6 seconds and
480p, validates the MP4, publishes and replays the sealed bytes, and verifies
that provider-private inputs and outputs are absent or empty. The approved real
invocation completed successfully. Its first test run then found that the
cleanup directory had been deleted rather than left empty; the post-success
assertion was corrected without repeating the paid invocation.

Normal test runs also execute `tests/grok_process_smoke.rs` without network or
membership consumption. It launches the compiled `grok-runner` as a detached
process against a contract-faithful fake CLI, forces the first artifact publish
to fail, removes launch-context availability, and then attaches from durable
spool evidence. The test requires one CLI invocation, two publication attempts,
one terminal manifest, and byte-identical terminal replay.

`tests/postgres_video_api.rs` additionally drives the public request through a
real isolated PostgreSQL schema, V3 admission and pricing, executor handoff, MP4
authority publication, terminal reduction, tenant-scoped status and download,
and exact six-second rating. It also proves idempotent replay, one ledger charge,
cross-tenant 404 behavior, and absence of raw data URLs from durable commands.

## Activation gates

Public xAI routes are implemented and default-off. Production activation still
requires all gates below to pass in the deployment environment:

1. xAI listener routing is independent from the OpenAI facade and is enabled
   only by `GATEWAY_ENABLE_XAI_VIDEO_API` (implemented and default-off);
2. the agentic CLI runs inside the production isolation and egress boundary;
3. runtime profiles bind account credential digest, operation descriptor
   digest, and adapter revision (implemented); a trusted deployment manifest
   must additionally authorize the provider and helper executable digests (the
   current supervisor self-pins the selected provider file only against later
   replacement);
4. the executor reads bounded `chat_history.jsonl` and invokes the provider
   receipt parser under its fenced launch lease (implemented for image and
   video generation);
5. staged input bytes are sealed and verified against their canonical hashes
   (implemented for Grok video generation);
6. image artifacts pass current decode validation through the artifact sink;
7. video artifacts use bounded MP4 validation and a true file-to-object-store
   streaming commit; validation is implemented, while the current truthful
   runtime descriptor remains `InlineBounded(256 MiB)`;
8. scheduler capacity, reservation, settlement, timeout, and ambiguous-result
   tests cover the Grok account (schema-scoped claim, separate video profile,
   V3 video-second settlement, and public-contract database E2E are implemented;
   uncertain expiry policy remains a deployment gate);
9. opt-in real image and image-to-video smokes pass through the durable Grok
   supervisor; the public-contract gateway E2E passes with a fake MP4 authority.

The current supervisor accepts the exact Grok image-generation and
video-generation schemas. Image-to-video has real durable-supervisor evidence;
reference-to-video has projection and fake-boundary coverage but no paid runtime
smoke. Image edits remain inactive. Video bytes currently traverse the existing
bounded 256 MiB durable spool and Files content response in memory; high-volume
production activation requires streaming/range delivery and a bounded download
concurrency lane rather than increasing that bound.

OAuth `auth.json` is copied into the execution-private provider home for local
verification. Production activation additionally requires an account-level
credential broker that serializes refresh-token rotation and persists updated
credentials without exposing the control spool to the agentic CLI.
