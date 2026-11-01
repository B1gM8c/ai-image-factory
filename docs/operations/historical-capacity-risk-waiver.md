# Historical capacity risk waiver

This operator records explicit acceptance of possible historical upstream
over-occupancy. It does not prove failure, release an original allocation,
cancel a remote task, retry generation, or change customer billing/results.
It is never invoked by a worker or an expiry policy.

The approved manifest must name each original allocation, submission and
execution, a single account and policy revision, and the reviewed result of
`executor_capacity_original_record_sha256(allocation_id)`. The digest covers
safe original metadata, not prompts, material URLs or credentials. The current
operator accepts at most 15 explicitly listed originals, only `uncertain`
Dreamina originals with held capacity, a deadline-quarantined intent with no
remote ID, and no final capacity evidence. Unknown is not converted to terminal.

`schema_version: 1`, `authorization_id`, `operator_reference`, `actor`,
`observed_before_ms`, and `expires_at_ms` are required. The exact
`risk_acknowledgement` is `unknown_original_may_still_occupy_upstream_capacity`.
Each `originals` member requires `allocation_id`, `submission_id`,
`executor_execution_id` and `original_record_sha256`. No wildcard or account-wide
selection exists. An expired or altered manifest fails without applying a subset.
The SHA-256 of the exact file bytes is an independent required command argument.

After explicit human approval of that exact scope, place the reviewed manifest
in a root-owned private regular file, beneath root-owned directories not writable
by group/others. Symlinks are rejected. The command defaults to dry-run:

```sh
capacity-risk-waiver /root/reviewed-manifest.json REVIEWED_SHA256
capacity-risk-waiver --apply --authorization-id APPROVED_UUID /root/reviewed-manifest.json REVIEWED_SHA256
```

The explicit command authorization UUID must match the manifest. This independent
operator is not included in the existing 14-binary standard application package.
Build it separately from the exact reviewed source, record and verify the binary
SHA-256, and validate that the deployed application and migration166 match before
any production apply. Never run `cargo run` on production. A local build or dry-run
receipt alone is not deployment authorization.

The dry-run only reads/locks and rolls back, producing prospective counters with
`applied: false`; it does not insert evidence. Apply appends the entire validated
set atomically. Repeating the identical authorization/file is idempotent; another
authorization for the same allocation fails. Migration verification is mandatory;
this command does not install migrations or change service/runtime bindings.

Capacity is `raw held - waived held`. The original raw counter still equals all
held allocation rows. New acquisitions lock the policy before reading waiver
counts, update the raw counter atomically, and enforce the effective upper bound
at commit. A later legitimate release of a nonfrozen original stops counting its
waiver, so it cannot free the slot twice. Frozen originals and their snapshots
remain unchanged. A policy revision switch remains blocked by any raw held
allocation, including waived originals; this patch does not migrate waivers.

Deploy with the separate capacity reconciliation scheduler/manual classification
and corresponding admin projection patch: show effective occupancy and an
explicit historical waived count. Do not label waived records as confirmed
failed/released, and do not subtract them from financial balances or customer
statuses. Local preparation or passing tests do not authorize production apply.
