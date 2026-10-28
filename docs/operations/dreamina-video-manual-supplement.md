# Manually supplement an uncertain Dreamina video

This operator workflow authorizes one new generation for an uncertain video. It
does not establish that the earlier request was rejected or reset its execution.
The original job, API ID, inputs, frozen quote and billing hold remain intact.
A separate record retains the old uncertainty and the new launch nonce/task ID.

Before using it, confirm that no original success, artifact, remote task or
success overlay exists and no consumer can still upload or refund an old result.
Do not substitute models, omit references or replace expired inputs with other
material. The database must already have every migration required by the signed
operator tool, with matching checksums. Run as the gateway
service identity with its protected database environment and the target video
profile's `remote-providers/<profile>.env`, as used by `provider-submitd@`.
Inject both through protected `EnvironmentFile` settings, never credentials in
arguments. Use a service-owned `0700` evidence root and `0600` files.

```text
factoryctl prepare-dreamina-video-supplement \
  JOB_ID ORIGINAL_SUBMISSION_ID ORIGINAL_EXECUTION_ID \
  EXPECTED_COMMAND_SHA256 EXPECTED_QUOTE_HASH OPERATOR_REFERENCE EVIDENCE_ROOT

factoryctl dispatch-dreamina-video-supplement AUTHORIZATION_ID EVIDENCE_ROOT
factoryctl poll-dreamina-video-supplement AUTHORIZATION_ID EVIDENCE_ROOT
```

Preparation checks the original inputs and freezes runtime identity/pins without
launching a generator. New plans also require these explicit settings from the
protected profile environment:

```text
PROVIDER_SUBMITTER_CLI_WALL_TIMEOUT_MS=540000
PROVIDER_SUBMITTER_PROVIDER_TIMEOUT_MS=600000
```

Both values must be positive integer milliseconds, at most one day; the CLI
budget must be at least 60 seconds below the provider budget. The values above
allow nine minutes for submit-time material uploads and the acceptance receipt.
They do not make a generation retry safe or prove that submission will succeed.
The reviewed `plan.json` schema version 2 freezes both values in `submit_budget`,
outside the unchanged database runtime identity. Dispatch rejects missing or
changed budgets before claiming its one allowed send. It uses the frozen CLI
budget for input staging, the command wall limit, the new launch's absolute
deadline and its bounded observer wait. The new deadline starts after acquiring
the credential guard; the original execution's expired deadline remains intact.
Query-only polling retains its separate 60-second limit.

Legacy plans without a reviewed budget cannot be dispatched by the new tool.
Do not edit their private plan, reset the claim or erase evidence to get past
this check. Already dispatched legacy plans can still recover their genuine
accepted ID and poll without the new configuration fields. Review an unused
legacy authorization separately; create this incident's new authorizations only
with the reviewed version 2 preparation tool.
 The private plan and journals contain prompts, arguments
and signed references; keep them out of ordinary logs and source control.

Dispatch must obtain its durable single-use claim before releasing the runner.
Timeouts, unknown responses, restarts or failed launches never authorize another
generation. Recover the matching accepted identifier from its durable journal or
remain uncertain. Polling only queries that accepted identifier. Pending results,
failures and incomplete downloads preserve uncertainty and the original hold.
Retain the frozen signed runner tree until publication; if a later application
upgrade changes the active runner pin, polling must explicitly use the original
frozen runner path and hash. It must not adopt a different binary implicitly.

Publication checks the new identifier, original command/quote/material binding
and retrieved MP4. API duration and billing units remain the original values;
measured parameters are audited separately. The final video is an immutable local
artifact served by scoped `/v1/files/ARTIFACT_ID/content`, with tenant, project,
actor, byte-size and SHA-256 checks. It does not rely on a mutable upstream CDN.

Delivery, usage, rating, ledger and audit records settle the original quote in
one transaction. Verify a unique original debit and the consumer's downloaded
hash. Do not manually edit the job, hold, receipt, fence, supplement or blob.
Keep unrelated completed orders unchanged.

## Recover a genuine acceptance when CLI stdout is missing

An exit status of zero with empty stdout does not prove that no request reached
Dreamina. The operator records durable uncertainty and a missing-receipt record;
it must not release a second generation or invent a submit receipt. Keep the
original gate, launch, claim, nonce and account log unchanged.

When the same execution has a genuine upstream acceptance in the private CLI
account log, use an independently reviewed recovery bundle:

```text
factoryctl recover-dreamina-video-supplement-acceptance \
  AUTHORIZATION_ID EXISTING_EVIDENCE_ROOT RECOVERY_BUNDLE_ROOT \
  EXPECTED_REVIEW_JSON_SHA256
```

This command imports existing query-only evidence. It never submits, retries,
cancels or refunds a generation. The bundle must retain the original CLI log
bytes, actual query stdout and execution audit, retrieved video bytes and the
review manifest. Supply the reviewed manifest digest as an argument; do not edit
the original gate or write query output into `submit-stdout.json`.
This is operator-reviewed source evidence, not a provider-signed attestation.
Keep the original root-protected archive; copy the reviewed bundle into a new
service-owned private directory (0700, regular single-link files 0600).

Recovery must bind the exact original authorization and nonce, command, quote,
account and runtime pins to the executed gate. The start and accepted account-log
records must identify the same invocation, with matching trace, input paths,
material bytes, prompt and generation parameters within that gate's execution
window. The actual query must name that accepted upstream ID and its prompt must
match the original bytes. Missing or ambiguous correlation fails closed; a
matching prompt alone is insufficient. Query/history fields that are absent do
not count as verified parameters.
The current recovery contract covers `multimodal2video` and requires a successful
query with one retrieved video. Pending results do not authorize publication.

The imported query receipt has an explicit recovery source and independent
private evidence. It does not become an invented submit stdout. Acceptance and
publication retain the existing leaf, cancellation, original-hold and unique
settlement checks. Verify the consumer's original task ID, one original debit
and actual downloaded bytes after publication. A completed import is idempotent;
it does not authorize a second dispatch or an additional customer charge.

## Recover the original normal-submit video archive

A normal submit with empty CLI output and a closed submit deadline has no
supplement authorization. Use its original query and downloaded MP4 only after
reviewing the original journal, account trace, command and frozen quote:

```text
factoryctl resolve-dreamina-video-archive \
  JOB_ID ORIGINAL_SUBMISSION_ID ORIGINAL_EXECUTION_ID \
  EXPECTED_REVIEW_JSON_SHA256 OPERATOR_REFERENCE PRIVATE_BUNDLE_ROOT
```

This path requires migration 157 and the matching gateway code. Deploying only
the operator is insufficient: the gateway must project the new archive mode as
a canonical artifact rather than also returning the historical CDN URL. The
current recovery scope is one 10-second, 720p `multimodal2video` original whose
submit intent is `deadline_quarantined` with `dreamina_submit_receipt_invalid`.
It does not create an acceptance receipt or a provider task, or call Dreamina.

The private bundle contains the untouched `command.bin`, `spec.json`,
`process-request.json`, `process-terminal.json`, `source-account-log.bin`,
`query-stdout.json`, `query-stderr.bin`, `execution-audit.json`, the reviewed
`capture-query-only.py`, `original-video.mp4`, `archive-audit.json`, and
`review.json`. The review freezes file hashes, the original job/submission/
execution/account/tenant, both distinct command hash namespaces, execution
binding, quote, native ID, source trace and line hashes, original execution
interval, account home hash and operator reference. Never reuse a different
order's evidence or modify an original gate to invent acceptance.

The loader validates the positive same-ID/same-prompt query, retrieval audit and
original MP4 before opening a business transaction. The transaction locks the
original job and funding, rejects competing authority or live capacity leases,
publishes the exact archive, closes capacity using an audited positive terminal
observation, and settles the original quote once. Original executor, receipt,
intent and deadline evidence stay unchanged. An exact replay returns the first
resolution; conflicting evidence fails. The original archive is retained even
when validation or the transaction fails. Related Dreamina native-authority
writes require READ COMMITTED so a waiter observes the shared identity-lock
winner; normal polling updates do not acquire that new lock.

Verify the normal scoped file download bytes and SHA-256, the original single
charge, and downstream delivery. Do not substitute a new generation, force a
refund, or edit database state when evidence is rejected.

## Use an independently attested operator tool

A standalone operator hotfix does not require changing `current` or restarting
the application fleet when its migration list and checksums match the installed
database. Use the existing `.github/workflows/operator-hotfix.yml` workflow:

1. Merge the reviewed source normally and wait for the exact main commit's full
   CI, including PostgreSQL 16/18 and recovery rehearsal, to succeed. Create a
   unique signed, protected `v*.hotfix.operator.*` tag at that commit and dispatch
   the operator workflow on that tag. Complete its normal `production-release`
   environment approval. The workflow checks the signed tag and exact main CI,
   formatting, media regression and supplement budget/process regressions before
   building and attesting the operator bytes.
2. Record the exact workflow run/attempt, artifact ID, archive size and archive
   digest. Download that artifact once. Require exactly `factoryctl` and
   `operator-manifest.json`; reject links or unexpected paths. Verify both files
   with the official GitHub attestation verifier, constrained to this repository,
   the `operator-hotfix.yml` signer, exact signed source tag and source commit:

   ```text
   gh attestation verify FILE --repo OWNER/REPOSITORY \
     --signer-workflow OWNER/REPOSITORY/.github/workflows/operator-hotfix.yml \
     --source-ref refs/tags/EXACT_SIGNED_TAG --source-digest EXACT_SOURCE_COMMIT
   ```

   Require manifest schema version 3, kind `operator_factoryctl_hotfix`, the same
   source commit/tag and run/attempt, the installed database schema and exactly
   these `intended_operations`: `prepare-dreamina-video-supplement`,
   `dispatch-dreamina-video-supplement`, `poll-dreamina-video-supplement`,
   `recover-dreamina-video-supplement-acceptance`. Check
   `factoryctl` size and SHA-256 against the attested manifest after extraction.
3. Copy only those verified files into a new root-owned protected operator
   directory, separate from `current`. Recheck their bytes on the production
   host. Execute that exact absolute `factoryctl` path as the existing gateway
   service UID with its protected `EnvironmentFile` settings and retained signed
   CLI/runner pins. Each command still verifies the database migrations and
   frozen account/runtime identity. Do not run an unverified local build, replace
   the runner, use a direct CLI generation or place credentials in arguments.
4. Prepare and review each authorized original job separately, then dispatch its
   authority once. Preserve the operator version, plan, launch, one-shot nonce
   and terminal evidence. A timeout does not reset authority; continue with poll
   only. Reconcile the consumer through its normal authenticated original-task
   path and verify the original debit and downloaded bytes exactly once.

This installs an operator tool only. Do not invoke bootstrap, Check/Apply,
application activation, schema changes or a `current` symlink switch for this
procedure. A tool that requires any unapplied or changed migration must instead
use the normal application migration/release workflow before operating.
