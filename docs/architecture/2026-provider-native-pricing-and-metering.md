# Provider-Native Pricing and Metering

Status: pricing administration and Usage read model connected; provider evidence
extraction rollout in progress

Last verified: 2026-07-24

## 1. Objective

AI Image Factory must preserve each provider's native accounting semantics while
offering one stable customer billing surface. The platform must answer five
different questions without conflating them:

1. What technical usage occurred?
2. What did the upstream provider actually charge?
3. What is the best available upstream-cost estimate?
4. What is the comparable official API cost for this workload?
5. What should the customer be charged?

The current economic kernel already freezes a price quote at admission, reserves
the maximum exposure, settles terminal outputs, and writes immutable double-entry
ledger records. This design extends that kernel; it does not replace it.

### 1.1 Implementation boundary

Implemented:

- versioned price books and immutable published components;
- provider-native usage facts with semantic idempotency keys;
- integer-only native-unit rating with explicit rounding;
- project, organization, and platform scope resolution;
- management APIs for draft, publish, retire, and catalog operations;
- a read-only preview API that resolves the published scoped version and runs
  the same native-unit rating code used by settlement;
- request-derived successful output facts written in the terminal reduction
  transaction;
- a v4 job-level frozen quote schema, multi-component quote planner, immutable
  quote lines, one job-level billing hold, rated usage lines, and links back to
  the exact provider usage facts;
- transaction-local quote persistence that validates idempotent replay and
  atomically reserves the tenant and currency billing account;
- OpenAI GPT Image 2 fixed-quality/fixed-size official token calculation for
  provider benchmarks and simulations. Customer-sale token versions fail
  publication readiness because Codex CLI does not expose an authoritative
  final quality and output-size fact for every admitted `auto` or ratio request;
  per-image customer rates remain exact and publishable;
- terminal v4 settlement for successful, failed, and multi-output GPT Image 2
  jobs, including usage facts, frozen-price rating, hold capture/release,
  balanced ledger posting, and idempotent replay;
- xAI image command extraction that binds `aspect_ratio` and `resolution` to
  the signed source command before v4 pricing can proceed;
- xAI video admission and settlement on v4 for the CLI-executable subset of
  `grok-imagine-video` and `grok-imagine-video-1.5`: the signed command freezes
  input-image count, requested duration, resolution, and effective aspect
  ratio; the customer quote reserves input images and output seconds once per
  output, then settles one immutable charge and balanced ledger posting;
- Dreamina image and Volcengine Ark image admission on v4, including durable
  command hashing, native provider-model versus execution-model separation,
  request-derived geometry, profile-aware price aliases, frozen quote lines,
  and atomic customer billing holds. Ark keeps its public API identity while
  the quote records the exact Dreamina price-book version used;
- Dreamina and Ark image terminal settlement E2E, including the managed account
  identity, external versus native API profile, provider-native usage fact,
  frozen dimensions, customer artifact, hold capture, balanced ledger posting,
  and idempotent terminal replay;
- Dreamina and Ark-compatible video admission and terminal settlement on v4,
  with duration, ratio, and resolution reconstructed from the signed CLI
  command, one `video_requested_second/second` customer fact per terminal output, MP4
  artifact validation, frozen customer-rate settlement, balanced ledger
  posting, and idempotent terminal replay;
- separate customer request and provider command hashes for routed Dreamina and
  Ark requests. The customer hash binds the public model, execution model,
  route ID, route revision, API profile, and provider command hash, while the
  command hash continues to prove the exact CLI payload;
- provider-native model and execution-model separation verified with a route
  whose Seedance execution model deliberately differs from the signed
  `seedance2.0fast` provider model.
- platform-owner-only official price source catalogs for OpenAI and xAI,
  immutable content-addressed source snapshots, current-catalog difference
  detection, per-item idempotent application records, and audit events;
- draft-only official price import: changed or new items can create benchmark
  price-book drafts, but the import path cannot publish or alter active prices;
- maker-checker publication for official imports: the user who applied a source
  snapshot cannot publish that draft. A different platform owner must review
  and publish it, and both denied and successful transitions retain the actor
  user, session, source snapshot, and reason in the identity audit log;
- safe historical rollback by cloning an active or retired immutable version
  into a new reviewable draft. Rollback lineage is immutable, publishing the
  clone uses the normal serialized cutover path, and a currently effective
  version cannot be retired without a replacement;
- a model-pricing administration UI for customer sale, provider cost, and
  benchmark views, including official source links, verification timestamps,
  snapshot hashes, selective difference review, and explicit draft generation;
- immutable provider-cost observations and subscription allocation pools, with
  separate actual, allocated, estimated, and benchmark purposes;
- database-enforced provider-cost ledger amounts, receipt-to-fact attribution,
  and allocation-period evidence boundaries;
- one authoritative owner per provider-cost usage fact, non-overlapping closed
  allocation periods, and period-scoped actual/allocated/legacy authority
  claims. Draft allocation lines remain editable and claim no authority;
- Grok/xAI `total_cost_usd_ticks` is captured inside the executor result
  manifest, persisted as immutable executor evidence, converted once after
  native-atom aggregation, and linked one-to-one to the resulting cost
  observation;
- every new actual-cost observation is bound to exactly one matching executor
  evidence manifest. The database revalidates provider, account, execution,
  receipt, fact, quantity, authority, confidence, and evidence identity before
  commit. Historical rows that cannot satisfy this contract remain explicitly
  `legacy_unverified` and cannot be created by current writers;
- a project-scoped Usage surface for ordinary users and an additional
  platform-wide operator view for administrators. Customer revenue, actual or
  approved allocated provider cost, gross margin, and cost coverage remain
  separate metrics; unavailable cost is never rendered as zero;
- scheduled price cutovers with database-enforced, non-overlapping half-open
  effective intervals. Publishing a future version atomically closes its
  predecessor and links it to the next scheduled version; cancelling a future
  version restores the predecessor interval without creating a pricing gap.
- a platform pricing-coverage control plane that enumerates adapter-supported
  model/API surfaces and independently checks exact route capacity, a published
  USD customer rate, an admission-compatible metering contract, provider-cost
  authority, and source provenance. Missing evidence fails closed instead of
  being rendered as a zero price or inferred readiness;
- explicit separation between runtime providers and official API benchmarks.
  `grok-cli` may compare against the `xai-grok` benchmark catalog, but this
  alias is accepted only for `provider_benchmark`; it cannot satisfy
  `provider_actual`, allocation, or customer-sale resolution;
- one canonical production operation vocabulary for official imports and
  settlement (`generation`, `edit`, and `video_generation`). API route names
  such as `images.generations` are translated only through an admitted command
  schema;
- an authoritative publication-readiness gate that runs inside the same
  serializable transaction and global advisory-lock boundary as publication.
  It requires at least one real platform model/API surface, validates the
  surface's request-dimension vocabulary, proves successful/failed/no-effect
  terminal coverage, rejects unreachable or ambiguous selectors, and prevents
  equal-precedence conflicts across active price books;
- versioned `PricingSurfaceContract` snapshots for Codex, Grok, and Dreamina
  image/video adapters. Publication resolves exact profile, provider model,
  public model, media, tier, and execution surface identities, stores a
  canonical contract revision plus lowercase SHA-256, and binds each published
  customer price to that immutable snapshot. Database triggers reject direct
  active inserts without a binding and reject quotes outside every bound
  contract. Contract schema v2 also marks the exact customer-sale bases that
  must be explicit: Codex image output, requested video seconds for Dreamina,
  and both input images plus requested video seconds for Grok video. Other
  observable bases remain available for provider cost and benchmark use without
  becoming accidental customer charges;
- one metering-authority rule reused by publication readiness, coverage, and
  admission. Customer prices must be exactly measurable; official estimates
  remain valid for provider benchmarks and estimates but cannot silently become
  customer charges. Already-active legacy estimated prices are rejected at
  admission without partial quotes, holds, outputs, or work items.

Not yet connected:

- exact upstream usage extraction for CLIs that do not expose billing evidence;
- Ark provider-cost token extraction for Seedance video. The connected Dreamina
  CLI v4 path is a configurable customer sale rate by output second, not a
  claim about Ark's official token cost;
- GPT Image 2 text/image input token rating for edits and streamed partial-image
  token adjustment;
- automated official-document fetch and parser verification. The current
  catalogs are curated, reviewed versions with explicit source dates;
- verified per-model Volcengine price catalogs. The source is visible in the
  UI, but import remains disabled until an auditable official rate table is
  available;
- exhaustive request-variant coverage across every allowed resolution,
  quality, duration, ratio, edit input, and output count. The current coverage
  view and bound `PricingSurfaceContract` prove the executable value domain for
  each registered surface; admission remains the authoritative fail-closed
  check for a concrete signed request;
- terminal Codex CLI output facts that reveal the effective quality and exact
  output dimensions for `quality:auto` and ratio-based requests. Until those
  facts are authoritative, GPT Image 2 output-token customer prices remain
  blocked even though the official API calculator is available for fixed
  benchmark inputs.

The v4 price book is authoritative only for routes explicitly admitted with the
customer-pricing-v4 contract. Dreamina and Ark image/video routes now
participate when their configured route and an explicit customer rate are
enabled; other v2/v3 routes retain their existing economics path. The UI must
expose that coverage state and must not present an unconnected provider/model
selector as billable through v4.

## 2. Non-Negotiable Invariants

- Store provider-native usage facts before converting them to money.
- Never recalculate a historical charge with a newer price.
- Never use a provider-reported cost as an unreviewed customer charge.
- Never label an estimate as an actual provider charge.
- Never publish an estimated quantity as a customer-sale billing basis.
- Never allow an official-price importer to approve the same imported draft.
- Never mutate an immutable historical rate or reopen it in place; rollback
  creates a new version with explicit lineage.
- Never retire the currently effective price without an atomic replacement.
- Never silently fall back to a zero price. Free service is an explicit price.
- Use integer quantities and integer monetary atoms in transactional paths.
- Preserve the raw provider evidence and its schema version.
- Treat rate limit, quota, budget, metering, rating, and ledger as separate
  domains.

## 3. Official Billing Semantics

The values below are source observations, not hard-coded runtime defaults.
Published rate-card versions retain their source URL and verification time.

| Provider | Workload | Native dimensions | Cost authority |
|---|---|---|---|
| OpenAI | GPT Image 2 generation/edit | text input tokens, cached text input tokens, image input tokens, cached image input tokens, image output tokens | provider usage when available; otherwise an explicitly marked official estimate |
| xAI | Grok image | media input images, output images, resolution/model | `usage.cost_in_usd_ticks` when the upstream response exposes it |
| xAI | Grok video | input images, output seconds, resolution/model | `usage.cost_in_usd_ticks` when available; otherwise native quantities times the published rate |
| Volcengine Ark | Seedream image | output images by model | native output count times the published rate |
| Volcengine Ark | Seedance video | input/output tokens and model variant | provider-reported tokens when available; otherwise an explicit estimate |
| JiMeng CLI | image/video | membership points, plan quota, generated outputs/seconds | observed points or plan usage; Ark API pricing is only a benchmark |
| Codex CLI | GPT Image | subscription quota plus generated media | observed subscription usage; OpenAI API pricing is only a benchmark unless exact API usage is exposed |

Current official references:

- OpenAI API pricing:
  `https://developers.openai.com/api/docs/pricing`
- OpenAI image generation cost rules:
  `https://developers.openai.com/api/docs/guides/image-generation`
- xAI pricing:
  `https://docs.x.ai/developers/pricing`
- xAI exact cost tracking:
  `https://docs.x.ai/developers/cost-tracking`
- Volcengine model pricing:
  `https://www.volcengine.com/product/doubao`
- Volcengine Seedance 2.0 token resource packs:
  `https://www.volcengine.com/activity/seedance2`

For the xAI video catalog verified on 2026-07-24:

- `grok-imagine-video-1.5`: USD 0.01 per input image, USD 0.08 per
  480p output second, USD 0.14 per 720p output second, and USD 0.25 per
  1080p output second;
- `grok-imagine-video`: USD 0.002 per input image, USD 0.01 per input
  video second, USD 0.05 per 480p output second, and USD 0.07 per 720p
  output second.

The CLI adapter does not claim the entire xAI API domain. Its current contract
opens only the signed, executable image/reference workflows, durations, and
resolutions that the installed CLI can run. Unsupported official dimensions
remain visible as benchmark catalog data rather than fake runtime capability.

## 4. Economic Views

### 4.1 Provider actual cost

The amount charged by the upstream provider for one request. This has the
highest cost authority. For xAI, retain the original USD ticks where
`1 USD = 10,000,000,000 ticks`, then derive ledger micros with a documented
rounding rule.

### 4.2 Provider estimated cost

The estimated upstream cost derived from published or contract rates when the
provider does not return final billing evidence. It retains the source and
confidence of every quantity and must never be presented as provider actual
cost.

### 4.3 Allocated subscription cost

CLI accounts may consume a monthly subscription instead of a per-request API
bill. Allocation is a management-accounting view:

```text
allocated request cost =
  subscription cost * request allocation weight / period allocation weight
```

It never overwrites provider actual cost and is never represented as official
API cost.

Actual and allocated cost are mutually exclusive for the same provider account,
job, currency, and overlapping accounting period. The database represents an
actual receipt as the point interval `[receipt_ms, receipt_ms + 1)` and an
approved allocation as the pool's full half-open interval
`[period_start_ms, period_end_ms)`. An allocation draft does not claim
authority; claims are created only when the pool closes. Adjacent periods are
valid, while overlapping closed periods are rejected under concurrent writes.

### 4.4 Official API benchmark cost

The cost the same workload would have under the provider's current public API
price. This is useful for margin and replacement analysis. A benchmark always
stores the exact rate-card version and confidence/source of every quantity.

### 4.5 Customer sale price

The price exposed by AI Image Factory. It can be cost-plus, fixed, promotional,
or contract-specific, but is always published as an immutable version and
frozen when admission succeeds.

## 5. Data Model

### 5.1 Rate books

`price_books` are policy containers:

```text
price_book_id
price_book_key
display_name
purpose             customer_sale | provider_actual | provider_estimated |
                    provider_allocated | provider_benchmark
scope               platform | organization | project
organization_id?
project_id?
currency
state               active | archived
created_at_ms
updated_at_ms
```

Resolution order for customer prices:

```text
project -> organization -> platform
```

Provider cost books are platform-scoped and provider-specific. Account-specific
contract discounts are separate overrides, never edits to the public benchmark.
Within the same scope, exact provider, route, model, and service-tier matches
outrank wildcard matches. Equal-precedence matches fail closed as ambiguous.

### 5.2 Immutable rate versions

Each version matches a route and model:

```text
price_version_id
price_book_id
version
api_profile
operation
provider_id
provider_model_id
public_model_id
service_tier
execution_surface   provider_api | provider_cli | manual_import
billing_mode        customer_rate | provider_reported | published_rate |
                    contract_rate | subscription_allocation |
                    membership_points
is_free
effective_from_ms
effective_until_ms?
state               draft | active | retired
source_url
source_checked_at_ms
```

Only `draft -> active -> retired` is valid. Publishing validates purpose and
billing-mode compatibility. Multiple published versions may share an exact
selector only when their half-open effective ranges do not overlap; PostgreSQL
enforces that invariant under concurrent writes. Publication splices a new
version between its predecessor and successor in one transaction. Cross-book
ambiguity still fails closed during resolution.

### 5.3 Price components

A version contains one or more declarative components:

```text
component_key
metric
unit
unit_size
unit_price_micros
quantity_source
rounding_mode
dimensions_json
```

Supported initial metrics:

```text
request
image_input
image_output
text_input_token
cached_text_input_token
image_input_token
cached_image_input_token
image_output_token
video_input_second
video_requested_second
video_output_second
membership_point
```

`video_requested_second` is derived from the signed request and may be used for
customer reservation and sale pricing. `video_output_second` means actual media
duration and requires provider-reported or `media_inspected` evidence; request
duration must never satisfy it. New MP4 artifacts persist `media_duration_ms` on
the append-only executor artifact authority. The derived second quantity uses
documented `ceil_to_second` rounding while retaining the raw milliseconds in
usage metadata; legacy artifacts without this evidence remain unrated.

`provider_reported_cost` is a usage fact, not a price component. It carries
native provider monetary atoms such as xAI USD ticks and is aggregated before
conversion to ledger currency.

`dimensions_json` contains typed, validated match dimensions such as quality,
resolution, width/height range, media input kind, or batch/service tier. It is
not an executable expression. Arbitrary scripts in pricing are prohibited.

### 5.4 Native usage facts

One provider receipt can yield multiple immutable usage facts:

```text
usage_fact_id
semantic_key
job_id
submission_id
receipt_id
provider_id
provider_account_id
execution_surface   provider_api | provider_cli | manual_import
metric
quantity
unit
quantity_source      provider_reported | request_derived | media_inspected | official_lookup
confidence           exact | bounded | estimated
evidence_path
metadata_json
created_at_ms
```

Examples:

```text
text_input_token=221
image_input_token=16384
image_output_token=1766
video_requested_second=8
video_output_second=7
membership_point=12
provider_reported_cost=200000000 usd_tick
```

### 5.5 Frozen quote and rating components

Admission stores every selected sale component and its maximum quantity in
`customer_price_quote_lines`. A quote is partitioned by independently terminal
work, such as `output:0`; its maximum is the sum of each partition's most
expensive terminal outcome. This preserves per-output rounding and prevents a
multi-output request from being under-reserved.

`customer_billing_holds` contains one job-level hold equal to the frozen quote
maximum. Settlement writes `customer_rated_usage_lines` and links each line to
its immutable `provider_usage_facts` through
`customer_rated_usage_fact_links`. Deferred database constraints independently
recalculate quote and rating totals; Rust-calculated amounts are not trusted by
the database.

Provider actual, allocated, and benchmark costs are written as separate cost
observations. Only provider actual and approved allocated costs may produce
provider-cost ledger entries.

An actual-cost write is accepted only through
`executor_provider_cost_evidence -> provider_cost_observation_sources ->
provider_cost_observations`. The source row is one-to-one with both the
observation and executor evidence manifest. Deferred constraints compare that
evidence with the exact submission, receipt, output, job, account, native
quantity, and immutable usage fact. Replays must present the same manifest.
Legacy receipt columns and unbound provider-cost ledger writes are guarded
against new writes; migration-only `legacy_unverified` source rows stay visible
for audit and coverage reporting but are not reported as `provider_actual`.

`provider_cost_authority_claims` is an immutable projection derived from source
records, not caller-supplied attribution. Actual claims are derived when a cost
fact is attached to an observation, allocated claims are derived only when a
pool closes, and legacy receipt-cost claims are derived when their ledger
transaction is inserted. A GiST exclusion constraint prevents different
authority kinds from overlapping for the same provider account, job, and
currency. A separate unique index prevents one usage fact from being attached
to multiple actual-cost observations.

The ledger amount is independently revalidated by deferred database triggers at
commit. A provider-cost transaction must contain exactly two balanced postings,
one positive and one negative, whose absolute amount and currency equal the
referenced observation or allocation line. A receipt linked to an observation
must also appear in that observation's immutable usage-fact set. Allocation
evidence must fall inside the allocation pool's half-open accounting period.
Application code cannot bypass these controls by supplying a different amount
or unrelated evidence.

## 6. Quantity Authority and Token Counting

Quantity authority is ordered:

1. provider-reported final usage;
2. provider-documented deterministic calculation;
3. request-derived bounded estimate;
4. operator-entered adjustment with audit evidence.

The platform must not claim exact token usage when a CLI omits it.

For GPT Image 2:

- preserve returned token categories when the upstream exposes them;
- reserve with the official quality/size estimator;
- mark CLI-only calculations as `official_lookup/estimated`;
- include text and image input usage for edits;
- account for every streamed partial image as additional image output tokens.

The implemented GPT Image 2 output estimator mirrors the official calculator:

```text
long_grid = low:16 | medium:48 | high:96
long_edge = max(width, height)
short_edge = min(width, height)
short_grid = round(long_grid * short_edge / long_edge)
grid_area = long_grid * short_grid
tokens = ceil(grid_area * (2_000_000 + width * height) / 4_000_000)
```

Accepted dimensions are the official calculator domain: both edges are
multiples of 16, total pixels are between 655,360 and 8,294,400 inclusive, no
edge exceeds 3,840 pixels, and the aspect ratio is at most 3:1. Known fixtures
include 1,024 x 1,024 as 196/1,756/7,024 output tokens for
low/medium/high. The runtime source marker is
`https://developers.openai.com/api/docs/guides/image-generation#gpt-image-2-output-tokens`.
Because the Codex CLI does not return the provider's final token receipt, this
deterministic official calculation remains `estimated`, not provider actual.

For xAI:

- retain `cost_in_usd_ticks` as exact provider evidence when present;
- aggregate exact ticks for the settlement boundary before converting to ledger
  micros, so per-request rounding cannot accumulate;
- keep media quantities as separate facts for diagnostics and benchmarking;
- do not derive a second "actual" amount from the public rate when exact ticks
  exist.

For JiMeng CLI:

- record observed points as `membership_point`;
- show Ark token/image prices only in the benchmark view;
- do not convert points to CNY without a versioned plan allocation policy.

## 7. Admission and Settlement

### Admission

1. Resolve project, credential, public model, provider route, and account group.
2. Resolve a published customer sale version.
3. Verify the provider command against its own hash, reconstruct pricing
   dimensions from that command, and require exact equality with the pricing
   intent. Do not use the customer idempotency hash as the command signature.
4. Verify the public/provider/execution model mapping against the frozen route
   attribution. Provider-native and execution model identifiers are separate
   fields and may differ.
5. Derive maximum quantities from those validated command parameters.
6. Freeze quote components and reserve the maximum customer exposure.
7. Reject the request if no explicit sale price is available.

The v4 path is fail-closed. A provider-priced token component cannot be quoted
from an image count or duration. It requires either a provider-documented
deterministic upper-bound lookup or another bounded quantity source. Until that
lookup exists for a model, v4 must not be enabled for that selector.

### Publication

Publication is a control-plane transaction, not a UI state change:

1. Start a serializable transaction and take the global `pricing:publish`
   advisory lock.
2. Resolve the draft against current platform model/API surfaces using the same
   canonical operation and execution-surface vocabulary as admission.
3. Reject unknown request dimensions, unreachable selectors, crossing selector
   predicates, and equal-rank ambiguity across price books.
4. Require a non-zero successful rate plus explicit failed and no-effect
   terminal paths. Free service must use the explicit free-price invariant.
5. Publish and close or schedule adjacent versions atomically.

The admin UI reads this same readiness result before enabling confirmation, but
the database transaction re-evaluates it. A stale browser result therefore
cannot bypass the publication gate.

### Settlement

1. Persist provider receipt and raw evidence.
2. Extract native usage facts idempotently.
3. Rate actual customer usage using the frozen sale components.
4. Record provider actual cost, allocated cost, and benchmark cost independently.
5. Capture the customer amount and release unused hold.
6. Append balanced ledger postings and seal the transaction.

Retries reuse the original quote. Reconciliation can append missing evidence or
compensating transactions but cannot mutate the original facts.

## 8. Admin Product Surface

Before implementing the UI, recheck the current OpenAI Platform billing and
usage interactions in the real browser session.

The connected pricing surface contains:

- `Pricing` model table with provider, model, media type, sale price status,
  cost coverage, margin, effective version, and last official verification;
- provider/model/media/status filters and search;
- customer sale, provider cost, benchmark, and all-price views;
- draft editing, preview calculation, publish confirmation, and retire action;
- guided per-model price creation that locks the public model, native model,
  API profile, operation, provider, and execution surface to a real routable
  entry instead of accepting free-form identity;
- server-authoritative publication preflight with matched-surface and request-
  dimension evidence; confirmation stays disabled when blockers exist;
- no editing of published versions;
- clear `exact`, `estimated`, and `unavailable` labels;
- source link and verification timestamp beside official rates.
- official price synchronization is a separate platform-owner workflow. Each
  check creates an immutable sync run, deduplicates normalized content into an
  immutable snapshot, records the retrieval method and evidence hash, and
  shows component-level old/new rates before any draft is created. The current
  sources use reviewed `curated_manifest` bundles because the official pages
  do not expose one stable machine-readable contract; this is intentionally
  not presented as live web scraping.
- a separate coverage table, patterned after OpenAI's compact limits tables,
  with provider/readiness filters and one row per model/API surface. It reports
  route, customer sale, metering, provider cost, and final readiness as
  separate columns. “Base contract ready” does not claim that every request
  dimension is priced.

The connected first Usage slice follows OpenAI's useful interaction pattern:

- 24-hour, 7-day, and 30-day ranges plus project scope;
- requests, outputs/seconds/tokens, customer spend, provider cost, and gross
  margin summaries;
- separate Activity and Cost views;
- administrator-only provider cost, gross margin, and coverage metrics;
- project-only customer spend and activity for ordinary users;
- platform-wide scope only for platform operators;
- explicit unknown-cost and unattributed-cost states.

The connected second Usage slice adds API key, user, provider, model, and
operation filters, 1-minute/1-hour/1-day time-series buckets, grouped
breakdowns, and filtered CSV export. Request counts use the job admission
timestamp; native usage and customer spend use their own immutable event
timestamps, so delayed settlement is not lost at a day or billing-window
boundary. The remaining drill-down follows a request through native usage
facts, frozen quote lines, rated usage, provider-cost evidence, and ledger
entries.

Billing controls intentionally use two separate boundaries:

- the organization billing-account credit limit is a hard admission bound.
  Only a platform owner can change it through the versioned control API. Every
  change requires an operator reason, one immutable
  `billing_account_limit_changes` row, one identity audit event, and an exact
  `control_version` increment. An account-scoped PostgreSQL advisory lock makes
  concurrent updates deterministic, while a database trigger rejects direct
  limit changes without matching evidence;
- the project monthly budget is a UTC calendar-month control with explicit
  `soft` and `hard` modes. Soft mode only emits threshold notifications. Hard
  mode checks settled spend, active customer-price reservations, and the new
  request's frozen maximum quote in the same admission transaction. The
  `project-spend-budget:{project_id}` advisory lock serializes limit updates
  and admissions across replicas; an over-limit request returns before a quote,
  hold, or work item can commit;
- the platform Usage view exposes a keyset-paginated organization-limit sheet.
  It queries organizations directly instead of downloading the user directory.
  Project Usage links to the existing project budget settings. Neither surface
  labels the hard credit limit as stored-value cash or merges it with provider
  quota observations.

Price publication also separates commercial truth from historical provider
evidence:

- a `customer_sale` version cannot be published retroactively. If its requested
  effective time is in the past, publication atomically moves the actual
  effective time to the publication transaction time before re-running surface
  readiness and overlap checks;
- provider benchmark versions may retain an official historical effective time
  because they are evidence, not a retroactive customer charge;
- the immutable transition audit records
  `requested_effective_from_ms`, the actual `effective_from_ms`, and
  `published_at_ms`, so billing review can distinguish intent, commercial
  effect, and operator action.

Billing integrity is a separate evidence plane, not another mutable ledger:

- a platform owner can start a manual scan and replay prior immutable results;
- every run uses one PostgreSQL `REPEATABLE READ` snapshot and an advisory lock
  so all findings share one cutoff while overlapping full-platform runs are
  rejected;
- the first check set compares billing-account held/captured counters with
  open holds and sealed customer-receivable postings, detects terminal holds
  older than 24 hours, proves every rated customer amount has the exact
  charge/postings/seal shape, verifies charged jobs have authentication
  attribution, proves every provider receipt owns one receipt-scoped cost
  obligation, ages only nonterminal obligations against their declared due and
  escalation times, and flags only facts already declared as
  provider-reported actual cost when they still lack a unique cost-authority
  claim after the 24-hour ingestion grace period;
- immutable run and finding rows preserve scanner version, check set, actor,
  timestamps, expected facts, and actual facts. The scanner never updates a
  balance, releases a hold, inserts a charge, or repairs attribution;
- the administrator surface follows the Billing history pattern: a compact run
  table and a read-only detail drawer. Customer Usage remains separate and
  cannot expose platform-wide integrity evidence.

Every provider receipt now creates exactly one mutable lifecycle record in
`provider_cost_obligations` and an immutable event history. The lifecycle owns
classification, due/escalation controls, settlement-claim identity, or a
strongly evidenced waiver; it never copies the monetary amount from actual or
allocation authority. Provider actual or allocated authority settles matching
obligations at the deferred transaction boundary after all fact, receipt, and
source links are sealed. Exact zero and positive sub-micro provider costs are
therefore settled authority, not waivers. An immutable `uncertain` receipt
remains historical evidence rather than an aging state: only its obligation
can become overdue, and age never creates an automatic waiver.

Customer refunds are now a separate financial reversal path:

- the original sealed customer charge remains immutable and usage facts are not
  rewritten. A refund creates a new balanced `customer_refund` ledger
  transaction linked through `reverses_transaction_id`;
- immutable `customer_refunds` evidence binds the original charge, reversal
  transaction, amount, actor, reason, idempotency digest, and request hash.
  Partial refunds are allowed, but the cumulative total cannot exceed the
  original customer receivable;
- the same tenant-and-currency advisory lock used by admission serializes
  refund changes with credit-limit decisions. `billing_accounts` retains gross
  captured and refunded counters while available credit and administrator
  views use net exposure;
- read operations require `billing:read`, refund mutations require
  `billing:refund`, and `admin:*` remains the explicit platform-owner wildcard;
- the integrity scanner verifies refund evidence coverage, source identity,
  payload binding, seals, posting shape, cumulative limits, and account
  counters. A reversal without evidence is a critical finding rather than an
  automatic repair.

Provider subscription allocation now has a receipt-exact close control plane:

- platform owners can list, inspect, preview, and create allocation drafts.
  Preview selection is exact across provider, provider account, currency,
  half-open period, tenant/project, API-profile alias, pricing operation,
  provider/public model, media kind, service tier, and execution surface;
- provider-actual authority excludes a candidate before weights are computed.
  The preview hash covers the immutable request and candidate set, and draft
  creation rechecks the hash under an advisory lock. Reusing an idempotency key
  with a different body or after candidate drift fails closed;
- a draft inserts no provider-cost authority, ledger transaction, posting,
  seal, or obligation settlement. Every draft line instead freezes the exact
  provider receipt and customer quote identifiers and hashes that made the
  candidate eligible. The pool also freezes a deterministic candidate-set
  hash;
- `provider_allocated` price versions describe eligibility and allocation
  dimensions. Their monetary total comes from provider invoice, contract, or
  subscription evidence, so a zero-valued component is valid and must not be
  replaced by a fabricated per-image price. Customer-sale and other paid
  per-unit books still require a positive success rate;
- migration `0092_provider_cost_allocation_close_guard.sql` rejects a closed
  pool unless residual is zero, every positive line owns exactly one sealed
  provider-cost transaction, and the line/ledger/seal set remains consistent
  at deferred constraint time. Migration
  `0093_provider_allocated_zero_rate.sql` keeps the database publication rule
  aligned with the application readiness rule above;
- migration `0094_provider_cost_allocation_receipt_snapshot.sql` makes one
  receipt the global cost-authority boundary, not provider/account/currency
  overlap. It backfills an immutable candidate snapshot on existing drafts,
  advances their optimistic `control_version`, binds every legacy line to its
  receipt and quote snapshot, and rejects ambiguous historical data;
- close requires a platform-owner identity, a distinct idempotency key, the
  current control version and candidate hash, and a supported invoice,
  contract, subscription, or statement reference plus lowercase SHA-256
  evidence hash. Only `successful_output` drafts with at least one line and
  zero residual are closable in the first release;
- the close transaction locks the pool and candidate receipts, replays snapshot
  identity, creates one receipt-scoped `provider_allocated` authority per line,
  writes and seals exact positive ledger coverage, settles the matching
  receipt obligation, records immutable closure evidence, and changes the pool
  to `closed`. Zero-valued lines receive authority and settle their obligation
  without fabricating a zero-value ledger transaction;
- a provider-actual write and allocation close race on the same receipt. The
  receipt row lock plus unique receipt-authority index allows exactly one
  authority to commit; the loser fails without a half-closed pool, duplicate
  ledger entry, or double-settled obligation. Allocation pool and line
  economics are immutable after creation, and closure evidence is immutable
  after close;
- the administrator UI exposes close only when the server state is actually
  closable. Empty, residual, or legacy job-basis drafts explain why close is
  unavailable. Closed details show the evidence reference, evidence hash,
  actor, session, and close time.

Migration `0091_customer_refunds.sql` is acceptable for empty or small
pre-production databases. Existing large production databases must not run it
implicitly during application startup: split deployment into
expand/validate/contract phases, set a bounded `lock_timeout`, inspect relation
sizes and lock waiters first, and build new indexes concurrently outside the
SQLx migration transaction before validating constraints and enabling writes.

## 9. Delivery Sequence

1. Completed: rate books, immutable versions/components, scoped resolution,
   management APIs, preview, and audit metadata.
2. Completed for OpenAI generation: signed-dimension metering, multi-component
   quote/rating, hold, settlement, and ledger.
3. Completed: exact provider-cost observations, allocation pools, executor
   evidence-bound xAI `total_cost_usd_ticks` ingestion, source-bound
   provider-cost ledger postings, period-scoped single authority, concurrent
   allocation-period exclusion, legacy write guards, and cost-coverage read
   models.
4. Completed for Dreamina/JiMeng and Ark-compatible image/video customer
   pricing: admission, native command verification, route identity, terminal
   metering, hold settlement, and ledger are connected. Ark token-priced
   provider cost remains separate and unimplemented.
5. Completed first product slice: pricing administration plus project-scoped
   Usage and platform-operator margin/coverage views with RBAC.
6. Completed: multi-dimensional Usage filters, grouping, time buckets, filtered
   CSV export, and request-level economic drill-down with platform/member RBAC.
   The detail snapshot joins frozen quote lines, native usage facts, customer
   rating, sealed ledger entries, and administrator-only provider costs.
7. Completed: model/API pricing coverage control plane with exact route-revision
   capacity, canonical pricing-operation mapping, admission-compatible
   metering checks, provider-cost authority, official benchmark aliases, and
   browser-verified filters and no page-level horizontal overflow.
8. Completed for the first routable surface: guided creation, authoritative
   publication preflight, atomic publication, and coverage verification for
   Codex `gpt-image-2` image generation. Continue backfilling only reviewed
   customer prices for other intended production surfaces.
9. Completed: `PricingSurfaceContract` schema v2, exact request-domain
   validation, immutable revision/hash binding, and required customer-metering
   bases shared by publication, admission, coverage, and the pricing form.
   Continue expanding generated witnesses for dimensions whose rates differ by
   quality, resolution, duration, ratio, or media input through the real quote
   and rating paths.
10. Completed: stable official catalog identities, versioned source sync runs,
    per-catalog transaction serialization, immutable source evidence and
    snapshots, batched price-book diff reads, component-level review, and
    draft-only import. Repeated and concurrent checks reuse one content
    snapshot while retaining distinct audit runs.
11. Completed: platform-owner organization billing-account controls with
    optimistic versions, immutable reasons and audit evidence, account-scoped
    serialization, database write guards, keyset pagination, and a
    browser-verified admin surface. Project monthly controls remain separate
    from organization credit and now support explicit soft monitoring or
    V4-pricing admission-time hard enforcement.
12. Completed: non-retroactive customer-sale publication with atomic effective
    time clamping, post-clamp readiness validation, and three-time audit
    evidence. Historical provider benchmarks keep their official effective
    dates.
13. Completed: immutable, snapshot-consistent billing-integrity runs for
    account counters, terminal holds, exact customer charges, and charged-job
    attribution, plus a narrow provider-actual fact-to-authority final-arrival
    check, with platform-owner APIs and a separate administrator review surface.
14. Completed: receipt-scoped provider-cost obligations, immutable lifecycle
    events, deferred authority settlement, evidence-backed waivers, uncertain
    outcome aging, and billing-integrity coverage/aging checks.
15. Completed: partial customer refunds with immutable evidence, idempotent
    reversal transactions, tenant-currency serialization, net account
    exposure, administrator APIs, and dedicated integrity coverage.
16. Completed: exact provider subscription-allocation preview and receipt-exact
    close, candidate authority exclusion, immutable receipt/quote snapshots,
    deterministic hashing, idempotent draft creation and close, platform-owner
    APIs, sealed ledger coverage, exact obligation settlement, residual proof,
    actual-vs-close race exclusion, browser-verified administration, and
    database evidence guards.
17. Remove the legacy single-component path only after reconciliation proves
    equivalent totals.

## 10. Verification Gates

- migration replay on an empty and populated database;
- immutability, duplicate-fact ownership, and concurrent non-overlap
  constraints;
- draft allocation followed by actual cost, and actual cost followed by
  overlapping allocation close;
- deterministic rounding and overflow/property tests;
- exact xAI tick conversion tests;
- OpenAI multi-component image estimate fixtures;
- Seedance token and Seedream per-image fixtures;
- quote replay across a price publication boundary;
- concurrent publish conflict, selector ambiguity, terminal fallback, unknown
  dimension, and missing-real-surface tests;
- tenant/project price resolution and RBAC tests;
- stale and concurrent billing-account control updates, direct SQL bypass,
  immutable control history, and platform-owner/member API authorization;
- customer-sale backdating rejection with historical provider benchmark
  preservation and transition-time audit assertions;
- billing-integrity snapshot consistency, concurrent-run exclusion, immutable
  evidence, keyset replay, member denial, and platform-owner API authorization;
- customer-refund partial/cumulative limits, concurrent over-refund exclusion,
  idempotent replay conflict, direct-SQL evidence bypass, account-counter
  agreement, and scanner detection of deliberately orphaned reversals;
- provider-allocation exact-dimension isolation, actual-authority exclusion,
  preview-hash/idempotency conflict, zero-total conservation, draft readback,
  no pre-close ledger/authority/obligation side effects, populated 0093-to-0094
  migration replay, receipt-snapshot drift, job-basis and residual rejection,
  exact positive and zero-line closure, concurrent close replay,
  actual-vs-close single-authority race, immutable closure evidence, and
  browser close-readiness/detail coverage;
- ledger balance and retry idempotency tests;
- browser E2E for filters, draft/publish/history, and usage drill-down.
