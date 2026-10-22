# Dreamina Seedance 2.5 API validation

This change enables Seedance 2.5 **text-to-video** on the existing asynchronous
Dreamina and Ark routes. It is the first production canary for the broader CLI
integration, not evidence that all eight CLI generation workflows are implemented.
The model discovery inventory covers all nine selectable image models and eight
selectable video models. Image-only Seedance 1.x workflows remain catalog-only
until their input preparation and execution adapters are available.

## Request and result

Use the production Factory API with a project API key bound to the Dreamina video
route. The default public model alias is `doubao-seedance-2-5-260628`; the native
CLI model is `seedance2.5`. The stable `doubao-seedance-2-5` alias is also accepted
when configured on the route. These are Factory aliases, not a claim that the CLI
and hosted Ark product expose identical capabilities.

```json
{
  "model": "doubao-seedance-2-5-260628",
  "content": [{"type": "text", "text": "An orange paper airplane flies slowly across a plain blue background. Fixed camera, even lighting, no text or people."}],
  "duration": 5,
  "resolution": "480p",
  "ratio": "16:9"
}
```

Submit once to `POST /api/v3/contents/generations/tasks`. Retain its `cgt-...` task
ID and poll `GET /api/v3/contents/generations/tasks/{id}`. An ambiguous submission
must be investigated using the journal and provider task evidence; do not create
a replacement paid task. The supported 2.5 text-to-video duration is 4–30 seconds
and resolution is 480p, 720p, or 1080p. Older model limits remain unchanged.

The query response reports the frozen requested model, resolution and ratio.
Those fields are not measured output dimensions. Validate duration, dimensions,
codec and playback separately using the produced video. No token usage is invented
when the CLI does not report it.

On success, `content.video_url` is the authenticated Factory path
`/api/v3/files/{artifact_id}/content`. Resolve it against the Factory base URL and
download with the same project key. Do not forward that key to an upstream CDN.
The existing Blog path streams the Factory video into a temporary file and uploads
it once to Qiniu; its final work URL is the Qiniu URL. Blog still needs an Ark
response adapter. Do not mark Blog integration complete based on a CLI receipt.

## Runtime and overhead

Use the [remote profile deployment procedure](github-release-deployment.md#dreamina-remote-profiles).
Account login provisions credentials and database profiles; it does not start
dispatcher, submitter or poller processes. Each account/operation profile shares
its runtime across models. Pin the same official CLI version and digest in the
gateway discovery/login path and both remote runtimes.

Generation submits with `--poll=0`. Backend pending checks are five seconds apart,
reducing their requested frequency by 80% relative to one second, at the cost of
up to four extra seconds before noticing completion. This is a scheduling bound,
not a measured throughput improvement. Client GET requests read database state;
they do not start another upstream CLI query. Ark metadata is projected in that
same SQL query without loading or parsing the full prompt payload.

The remote profile example uses one-second empty-queue checks and a ten-minute
upstream task budget, frozen when submission starts. Individual CLI submit/query
processes retain a separate sixty-second limit. Lease heartbeats continue during
the task; changing configuration does not extend already-submitted deadlines.

Model help discovery runs only on refresh, with at most two commands in flight
per refresh. Video sealing streams to the artifact sink, preserving hash and size
verification. No transcoding or extra Qiniu copy is introduced.

## USD tariff reference

The selected benchmark is [BytePlus ModelArk pricing](https://docs.byteplus.com/en/docs/modelark/model-pricing),
verified on 2026-10-22 (page updated 2026-10-19). For 2.5 without input video:

| Resolution | USD / million video tokens | Published USD / second example (16:9) |
| --- | ---: | ---: |
| 480p | 10.70 | 0.103 |
| 720p | 10.70 | 0.231 |
| 1080p | 11.70 | 0.569 |

Factory currently charges `video_requested_second`, not provider video tokens.
Use an explicit versioned customer tariff derived from these per-second examples;
do not label the resulting Factory charge as a measured BytePlus token bill.
At USD 0.103/second, the five-second canary costs USD 0.515 in Factory. BytePlus's
separately rounded five-second example is USD 0.514. The Dreamina account's actual
credit delta is separate provider-cost evidence, not this customer sale price.
No CNY exchange rate is involved. Reference-video pricing and minimum token rules
need their own tariff when those workflows are implemented.

The video pricing surface is revision 3. Existing prices for previously supported
Dreamina models keep their immutable revision-2 bindings. Publish the Seedance 2.5
customer tariff through the normal draft/publish flow so its exact model/API
surface receives a revision-3 binding. An older wildcard tariff bound only to
earlier models does not cover the new 2.5 surface; do not edit database bindings.

## Acceptance evidence

Before a paid call, require the signed candidate checks, normal Check/Apply and
full runtime verification, refreshed model mapping, published tariff coverage,
and model visibility through the chosen key's `/v1/models`.

Refresh the model catalog after switching the CLI and application versions: an
earlier catalog may have discovered 2.5 as disabled, and startup preserves that
state. Existing route revisions also retain their mappings. Create a new route
revision with the 2.5 alias through the console and verify the project/key uses it.

The free `postgres_video_api::dreamina` integration test uses a disposable real
PostgreSQL schema, the HTTP router, Workerd handoff, gated submit runner, actual
Dreamina codec/poll driver and a local fake CLI. It verifies single submission,
idempotent replay, metadata, tenant isolation, artifact bytes and settlement.
Its MP4 is a transport fixture; it does not validate model quality or real media
specifications. Real login, real generation, playback and credit delta are
separate production evidence.

Only the single explicitly authorized production API task may incur provider
cost. Record Factory task ID, upstream submit ID, terminal status, artifact hash,
actual video properties, before/after credits and the customer billing event.
Do not retry or regenerate if any part is uncertain.
