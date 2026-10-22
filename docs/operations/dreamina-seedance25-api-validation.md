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

On success, `content.video_url` is the HTTPS video URL returned by Dreamina.
Factory persists the provider result and settles the task without downloading,
transcoding or storing the video. The client must fetch this URL without the
Factory API key or owner cookies. The result remains scoped to the original
Factory tenant, project and actor when querying the task.

The CLI does not document a guaranteed URL lifetime. An absent upstream expiry
remains unknown; Factory's local artifact retention policy does not apply to this
URL. Consumers that need durable storage should copy the video promptly. Blog's
adapter should download the upstream URL with its existing public-URL validation,
then upload once to Qiniu and retain that Qiniu URL. No Factory media copy is
needed. Other providers' existing authenticated Factory artifact URLs are unchanged.

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
per refresh. Dreamina video queries omit `--download_dir`; successful URL evidence
uses the existing terminal reduction transaction for a single customer charge.
The video branch never writes the artifact sink. Image generation retains the
existing downloaded-artifact path.

## Migration and recovery

Migration 0136 stores immutable upstream URL evidence separately from local media
manifests. The existing resolution and terminal reduction transactions still own
success, quota consumption and billing. Remote and local delivery are mutually
exclusive; the API exposes the URL only after terminal settlement completes.

During the first deployment, keep Dreamina dispatch disabled until normal Apply
and runtime verification finish. The existing backup/restore path handles a failed
Apply before new traffic is admitted. After remote results exist, an older binary
that only understands local artifacts cannot serve those historical tasks. Drain
Dreamina submissions and pending reductions before any later rollback, and retain
a rollback build that understands migration 0136 and remote result reads. Do not
switch back to the schema-135 application against a database containing remote
video completions: its migration verifier rejects schema 136.

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
idempotent replay, metadata, tenant isolation, byte-preserved URL delivery and
single settlement without local media artifacts. Its URL is a fixture; it does
not validate upstream URL access, model quality or real media specifications.
Real login, real generation, playback and credit delta are separate production
evidence.

Only the single explicitly authorized production API task may incur provider
cost. Record Factory task ID, upstream submit ID, terminal status, returned URL evidence,
actual video properties, before/after credits and the customer billing event.
Do not retry or regenerate if any part is uncertain.
