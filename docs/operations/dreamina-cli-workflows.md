# Dreamina CLI generation APIs

The adapter follows the official Dreamina CLI 1.4.21 generation commands. Native
requests use CLI parameter names with underscores. Inputs that are local paths
in the CLI become HTTPS or data URLs in the API. The gateway fetches and validates
these inputs, freezes their content hashes and order, and stages private files
for the selected account. Clients cannot name server files.

## Endpoints and workflows

`POST /v1/dreamina/images/generations` accepts these `workflow` values:

| Workflow | Generation parameters |
| --- | --- |
| `text2image` | `prompt`, `model_version`, `ratio` or `width` + `height`, `resolution_type`, `generate_num` |
| `image2image` | `images` plus the text-to-image parameters |
| `image_upscale` | `image`, `resolution_type` (`2k`, `4k`, `8k`); fixed upstream model |

The image endpoint retains the existing synchronous image response. Newly
admitted `text2image` and `image2image` requests for 2–10 images use one native
CLI submission with `generate_num=N` and one real upstream task ID. Each image
retains its own output, artifact and customer settlement record. Single-image,
upscale and video requests keep their existing execution paths. Historical
version 1 frozen commands retain their original per-output behavior across an
upgrade; only version 2 image commands select native batching.
Public idempotency identity retains the historical image request hash, while
execution and pricing evidence bind the actual frozen command hash. Replaying
an older key therefore retains its original admission and execution path.

Only the batch leader can acquire a submit lease. All members have immutable
output indices, and followers cannot fall back to independent paid submissions.
Submission timeout or an unknown receipt follows the existing durable submit
journal; it does not authorize a retry or a replacement task. Restart recovery
queries the same task and resumes artifact publication. Downloaded files are
validated together and sorted by raw filename bytes before assigning output
indices; already committed images must match their stored content hashes.
Follower results, including explicit missing-image failures, are committed
before the leader artifact. Database guards enforce this order so deadline
recovery cannot encounter a committed leader with unresolved batch members.

Completed images are settled individually and replayed idempotently. Missing
images become explicit failed outputs; partial success returns and charges only
delivered images. Neither missing images nor storage failures trigger paid
regeneration. A batch's upstream cost evidence, when available, belongs to the
single real upstream task and must not be duplicated across image receipts.

`POST /v1/dreamina/videos/generations` accepts these `workflow` values:

| Workflow | Generation parameters |
| --- | --- |
| `text2video` | `prompt`, `model_version`, `ratio`, `duration`, `video_resolution` |
| `image2video` | `image` plus video generation parameters; Seedance 2.5 requires omission of `ratio` |
| `frames2video` | `first`, `last` plus video generation parameters; Seedance 2.5 requires omission of `ratio` |
| `multiframe2video` | `images`, `video_resolution`, per-segment `transitions` or `transition_prompt` + `transition_duration`; two images may use `prompt` + `duration` |
| `multimodal2video` | `image`, `video`, `audio` arrays plus video generation parameters; `prompt` is optional |

Missing `workflow` retains the existing text-generation request shape. Session
selection and polling belong to Factory: omit `session` and `poll`. Explicit
integer zero is accepted for compatibility; other values are rejected. An API
GET reads the stored task state rather than starting another CLI query.

Videos return a task ID. Query `GET /v1/dreamina/videos/{task_id}` until terminal.
On success, `content.video_url` is the original upstream HTTPS video URL. Factory
does not download or store that video. The upstream does not provide a verified
expiry in the current receipt, so no lifetime is promised. A consumer needing
durable media, such as Blog, should copy the successful result to its own storage
once and save that durable URL.

## Model and media constraints

The model catalog includes the selectable CLI model versions and the fixed
`image_upscale` and `multiframe2video` workflows. Catalog discovery does not enable
a route or publish a price. `/v1/models` is scoped to the caller's configured
routes and current capabilities; administrators must configure the applicable
workflow price before accepting requests. Public route aliases may differ from
the upstream `model_version`.

To expose a newly discovered model, save its route mappings and explicitly set
that saved revision as the platform route before publishing its price. Catalog
refresh alone does not change this binding. Setting the platform route leaves
existing project and API-key route bindings unchanged; configure the caller's
route separately when enabling access.

Model-specific input counts, duration ranges and resolution combinations are
validated before submission. Examples include Seedance 2.5's 4–30 second output,
480p/720p/1080p resolutions, and up to 30 images, 10 videos and 10 audio inputs
with 50 total inputs. Older models retain their smaller limits. First-frame and
first/last-frame ratio inference is preserved rather than silently substituting
a requested aspect ratio.

The gateway currently accepts PNG/JPEG/WebP images, non-fragmented MP4 with
H.264/AVC video, MP3 audio and PCM WAV audio. The declared media type must match
the content. Input duration and dimensions are measured by the server, not
trusted from a client. Files and total request bytes also have resource bounds;
support for a model's input count is not an exemption from those bounds.

Multiframe requests use 2–20 images and N−1 transitions. Each transition is 1–8
seconds and the total is at least 2 seconds. Decimal durations must resolve to
whole milliseconds. Quota reserves ceiling seconds, while the independent
`video_requested_millisecond` pricing quantity retains the exact duration. For
example, 2.5 seconds reserves three quota seconds but is priced as 2500
milliseconds. Existing second-based prices and historical records are unchanged.

## Ark-compatible requests

`POST /api/v3/contents/generations/tasks` and
`GET /api/v3/contents/generations/tasks/{task_id}` use the Ark task shape:

- Text content selects text-to-video.
- A first-frame image selects image-to-video; an omitted image role also denotes
  the first frame.
- First- and last-frame roles select frames-to-video, preserving semantic order
  even when the content list is ordered differently.
- Reference image/video/audio roles select multimodal-to-video. Mixing these
  references with first/last-frame roles is rejected.

The native multiframe workflow is not represented by an invented Ark role.
Ark fields that the CLI cannot honor return an explicit unsupported-parameter
error. In particular, automatic duration (`-1`) is not silently changed to five
seconds. Successful Ark responses return the upstream URL in `content.video_url`.
Requested resolution, ratio and duration are read from the frozen request, not
inferred from quota reservations. A fractional duration remains a JSON number
such as `2.501`, while whole-second values retain their integer representation.

Reuse the same `Idempotency-Key` when querying the outcome of an interrupted
submission. Replaying an identical admitted request reuses its task; changing
the body under that key conflicts. A timeout or uncertain submission must not be
treated as permission to create another paid task.

## Verification boundary

Video status queries distinguish `running`, `uncertain`, `failed`, and
`succeeded`. `uncertain` includes `generation_uncertain` and requires operator
reconciliation; it is never permission to retry or create a replacement task.
An explicit one-time web safety refusal is `failed` with
`dreamina_compliance_confirmation_required`. Complete the website confirmation
before any future request; the rejected task is not retried.

### Historical compliance refusals

Migration 0141 adds a narrow operator settlement for a single-output Dreamina
video that was previously quarantined as uncertain despite a complete, explicit
4010 refusal. Review the original stdout independently and record its SHA-256.
After deploying the compatible build, run with the normal protected database
configuration:

```sh
factoryctl resolve-dreamina-compliance-rejection \
  JOB_UUID SUBMISSION_UUID EXECUTOR_UUID REVIEWED_STDOUT_SHA256 \
  OPERATOR_REFERENCE EXISTING_PROFILE_JOURNAL_ROOT
```

The command only reads the existing, bound journal and process evidence. It
rejects active, accepted, ambiguous, truncated, charged, or mismatched evidence.
It preserves the immutable executor decision and receipt, appends an audit
resolution, and atomically releases the full customer hold, quota, grant
reservations, and local provider capacity. It does not invoke the CLI. Exact
repetition returns the same audit receipt; a changed identity, digest, or
operator reference conflicts. Do not use cancel/retry commands or direct table
updates for these quarantined orders. Notify the client to GET the original
task after settlement; an existing webhook event with no deliveries does not
prove the client received it.

`GET /v1/dreamina/videos/{task_id}` returns an optional `settlement` only for
this audited, fully released refusal. It contains `state: released`,
`resolution_audit_id`, `job_id`, the original `request_id`, `hold_id`, `currency`,
and decimal-string `held_micros`, `released_micros`, and `captured_micros`.
The read uses the same tenant, project, and actor scope as the task. Clients must
bind both the top-level task ID and receipt job ID to their stored provider task,
require full release and zero capture, and apply any real local precharge release
once using the audit identity. Absence of a receipt is not proof of release.

### Offline and production checks

Local fixtures exercise parsing, staging, idempotency and settlement without
calling the paid upstream. Deployment requires successful CI for the merged
commit and the normal signed-candidate Check/Apply process. Only a separately
authorized production API task proves real upstream generation, playable media
and actual account charges; catalog discovery and fixture success do not.

Native batch PostgreSQL tests opt in through `TEST_DATABASE_URL` and create
isolated schemas. Use a disposable local PostgreSQL cluster, never production
credentials. Native batching requires migration
`0140_dreamina_native_image_batches.sql` before activation. Older binaries do not
understand version 2 commands: stop new batch admissions and finish or reconcile
in-flight native batches before rolling the binary back. Do not delete their
membership or recreate their submissions. Version 139 binaries also reject a
version 140 database at startup, even after all batches finish. Rollback must
use a compatible build that retains migration 0140, or the supported backup
and database-restore recovery path while admission is closed. Reconcile any
post-upgrade business facts before restoring a pre-upgrade database; never
change migration metadata to make an old binary start. Code and fixture
verification alone do not demonstrate real CLI result naming, actual image
counts or upstream charges.

Sources: [official CLI guide](https://bytedance.larkoffice.com/wiki/FVTwwm0bGiishxkKOoScdHR2nsg)
and [Ark video task API](https://ark.volcengine.com/region:cn-beijing/docs/create-video-generation-task-api).
