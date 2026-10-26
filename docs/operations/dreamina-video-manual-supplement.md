# Manually supplement an uncertain Dreamina video

This operator workflow authorizes one new generation for an uncertain video. It
does not establish that the earlier request was rejected or reset its execution.
The original job, API ID, inputs, frozen quote and billing hold remain intact.
A separate record retains the old uncertainty and the new launch nonce/task ID.

Before using it, confirm that no original success, artifact, remote task or
success overlay exists and no consumer can still upload or refund an old result.
Do not substitute models, omit references or replace expired inputs with other
material. Install the signed application/migration normally. Run as the gateway
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
launching a generator. The private plan and journals contain prompts, arguments
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
