# Blog local Codex CLI edit acceptance — 2026-09-08

## Scope and compatibility

The optional `openai-codex-edit-cli-v1` execution profile uses the existing native
Codex app-server image tool. Existing `openai-codex-edit-inline-v1` profiles keep
their direct HTTP behavior. The new profile is created explicitly with
`factoryctl provision-codex-cli-edit-profile`; existing profile identities and
disabled shared account-operation state are preserved transactionally.

No API shape, Factory UI, Blog code, production profile, or database schema is
changed by this patch. It reuses verified staged inputs, auth isolation, durable
execution, native artifact validation and reduction. CLI edits do not perform
automatic authentication/image retries and never fall back to HTTP. This is
`semantic_mask`, not pixel-locked inpainting or real PSD layers.

## One real browser-to-provider edit

Blog's existing browser flow selected the real Chinese bbox item `粉色连衣裙`,
created its PNG mask with canvas, and submitted exactly one edit with one
original image, one mask, `n=1`, `size=auto`, and public model `gpt-image-2`.

Prompt: `仅将粉色连衣裙改成淡蓝色，保持衣服款式、人物面部、姿势和海滩背景不变。`

- Blog task: `aiimg_3f76149897e5f11f`.
- Factory job: `b2021d9b-655d-4326-a70e-ce4bdfc3444d`.
- Provider submission: `271ef4ac-9bb0-4d12-9861-4a94c7ad98c0`.
- Executor execution: `fdac9b51-063f-4071-aa17-1d2cab584cc9`.
- Native CLI: `codex-cli 0.153.4`, macOS arm64, PID `47353`.
- Native binary SHA256: `b973d440acac501fd2594a43e7ca9ce41e0a65b9dfb28d0d7a7837c99e1261e3`.
- Provider process start identity: `macos:1788843439:763468`.
- CLI startup preflight accepted the existing strict config and image-tool
  configuration, with zero model turns. Its default orchestration model was
  `gpt-6-astra`; this is distinct from the public image model and the bbox model.
- Provider/Factory terminal state: `succeeded`; one auth attempt, no second
  attempt, one output. Local test ledger: held `0`, captured `40000` micros.
  This is a disposable test allowance, not a purchase or production charge.
- Blog independently confirmed the rendered light-blue dress and settled task.

The native app-server path accepts success only after exactly one bound native
image-tool completion and reading its authorized output. PID/start identity,
attempt records, staged inputs, durable result/terminal, and output bytes were
observed in this real run. Raw successful protocol event JSON/tool call ID was
not retained; the temporary CLI home was cleaned on completion. Do not present
reconstructed events as raw captured logs or claim a separately measured
upstream image-tool duration.

## Timing, with queue delay separated

All timestamps below are Unix milliseconds unless explicitly fractional.

| Boundary | Timestamp | Interval |
|---|---:|---:|
| Factory job created | 1788843203149 | — |
| Work item ready | 1788843203242 | 0.093 s |
| Submission prepared | 1788843435514 | 232.272 s waiting |
| Work item handed off | 1788843435522 | 0.008 s after prepared |
| Executor leased | 1788843435673 | 0.159 s after prepared |
| Executor started | 1788843435679 | 0.165 s after prepared |
| Native CLI process started | 1788843439763.468 | 4.084 s after executor start |
| Executor/provider succeeded | 1788843530368 | 90.605 s after native start |
| Factory result/settlement completed | 1788843530725 | 0.357 s final reduction |

Total Factory time: **327.576 s**. Native-process start to Factory completion:
**90.962 s**. These are one-sample measurements, not P50/P95 or an SLA.

The long wait was the local quota-freshness admission gate, not CLI warmup:

- The route had `unknown_quota_policy=block`, freshness `300000` ms.
- The previous successful quota-refresh command had returned by
  `2026-09-08T04:48:10.411Z` (observed data cannot be newer than that completion).
  The job arrived at `04:53:23.149Z`, at least 312.738 s later: already stale.
- The next real quota snapshot was observed at `1788843435354`; submission
  preparation followed **160 ms** later.
- Workerd was already running at `04:42:31Z`; the route/profile were enabled,
  account capacity was one, and this was the only local image job.
- The existing claim query requires fresh quota when the policy is `block`
  (`src/admission/postgres/operations.rs`); neither the per-job CLI nor its
  runner child existed during the 232.272 s ready wait.

For this isolated setup, refresh the account before another explicitly
authorized test. For unattended operation, arrange periodic refresh through
the existing single-account management API, below five-minute intervals.
Gateway's existing 60-second loop refreshes Codex credentials, not quota
snapshots; no existing quota timer/configuration was found. A local OS timer
calling `POST /admin/v1/provider-accounts/{id}/quota-refresh` every 120 seconds
was proposed, not installed. Prefer that API over periodically starting
factoryctl, whose initialization also performs management startup recovery and
route reconciliation. No
scheduler rewrite, relaxed quota gate, larger TTL, extra generation, or automatic
retry was introduced to hide this delay. No warm-queue latency claim was tested.

## Geometry and precision boundary

Original, browser-created mask, and final PNG all decode to **853 × 1844**.
Mask alpha values are exactly `0` and `255`. Its transparent region is exactly
`[197,577,664,1248]`, comprising `313357` pixels.

| Artifact | SHA256 |
|---|---|
| Original | `43fc9efc08fd5f17471d7b083223d62a7378e4d11b052012adfed251e6b1c2fb` |
| Mask | `ccb4dea9d823e93f4d4da336dd3b0cb4d26534f131c7ad679b6624dff66d0a2b` |
| Output, 2363709 bytes | `f280683b37f93d404ae8faa85eb93da36a958a532ae7db7bc2685d60c5f8fb98` |

Visual inspection confirms pink-to-light-blue dress recoloring with broadly
similar composition. **Non-selected pixels are not locked**: 99.7854% of the
1259575 outside-mask pixels have a nonzero RGB difference; mean absolute channel
difference is 9.9015/255 (inside-mask mean: 17.1439/255). These are pixel-change
statistics, not perceptual accuracy scores. Background/body details also vary.
No local image compositing or second model call was used to improve the result.

## Verification and remaining gates

- Codex supervisor: 42 passed, one pre-existing process stress test ignored.
  Includes four new CLI tests: native-tool artifact path, mask/reference order,
  tampered input rejection, no auth retry/HTTP fallback, and revision checks.
- Profile binding: 5 passed; factoryctl: 21 passed.
- Real PostgreSQL provisioning suite: **14 passed** in isolated test schemas,
  including exact replay, old HTTP preservation, disabled profile preservation,
  disabled shared edit capability rejection, and atomic rollback.
- Five consumer/provisioning binaries built; formatting and diff checks passed.
- Full library, serial: **590 passed, 1 failed, 7 ignored**. The same Dreamina
  macOS `KeychainUnavailable` failure at credential replacement line 1032 was
  independently reproduced on clean `origin/main` baseline `415e482`.
- Parallel full-library execution additionally hit process-test deadlines;
  those failures disappeared in the serial run. Do not call that parallel run
  passing.
- Strict Clippy fails on both baseline and patch. JSON diagnostic comparison
  found **136 emitted error diagnostics on each, zero new/removed diagnostics**
  (includes duplicate lib/lib-test reports); no lint suppression was added.
- Independent review found the shared disabled-operation provisioning issue;
  it was corrected with an atomic opt-in-only conflict check and real PG test.
- CI is not claimed green. No production deployment was performed. Local
  Gateway, segmentd, PostgreSQL and edit/generation consumers remain available
  for Blog inspection; the single real edit allowance is already consumed.

Private runtime evidence is retained outside Git in the local integration
directory (`edit-readiness.json`, `edit-pixel-evidence.json`, native runner
records and `clippy-comparison.json`). Credentials and input/output images are
not committed to the repository.
