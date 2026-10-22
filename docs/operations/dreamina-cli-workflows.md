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

The image endpoint retains the existing synchronous image response. A request
for N images uses the existing N-output execution path, with one upstream task
per output. Each CLI submission uses `generate_num=1`. Completed outputs retain
their own settlement records; a failed output does not trigger paid regeneration.
This is not a single upstream batch.

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

Local fixtures exercise parsing, staging, idempotency and settlement without
calling the paid upstream. Deployment requires successful CI for the merged
commit and the normal signed-candidate Check/Apply process. Only a separately
authorized production API task proves real upstream generation, playable media
and actual account charges; catalog discovery and fixture success do not.

Sources: [official CLI guide](https://bytedance.larkoffice.com/wiki/FVTwwm0bGiishxkKOoScdHR2nsg)
and [Ark video task API](https://ark.volcengine.com/region:cn-beijing/docs/create-video-generation-task-api).
