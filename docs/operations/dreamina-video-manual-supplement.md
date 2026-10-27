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

## Use an independently attested operator tool

A budget-only operator hotfix does not require changing `current` or restarting
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

   Require manifest schema version 2, kind `operator_factoryctl_hotfix`, the same
   source commit/tag and run/attempt, the installed database schema and exactly
   these `intended_operations`: `prepare-dreamina-video-supplement`,
   `dispatch-dreamina-video-supplement`, `poll-dreamina-video-supplement`. Check
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
