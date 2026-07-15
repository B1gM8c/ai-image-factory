# Phase 1G: Execution Binding And Durable Capacity

Status: implemented on the internal executor path. Public Images V2 traffic
remains disabled until the work handoff, canonical reducer, customer artifact
publication, and real API smoke gates are complete.

## 1. Boundary

An official API model is not an execution credential. Before a provider side
effect, `workerd` selects one internal execution profile and freezes it into
every output-level provider submission. `executord` may claim only the exact
profile it loaded from PostgreSQL.

The profile freezes:

- provider ID and command schema;
- adapter revision;
- credential pool and provider account identity;
- opaque credential reference and revision;
- resource policy ID and revision.

Secret bytes, host credential paths, prompts, and public API keys are not stored
in the profile. The credential reference names a secret-broker or mounted-secret
identity. The current process runtime maps that opaque reference to a private
Codex auth home and remains a development/test isolation tier.

## 2. Durable Data

Migration `0013_executor_execution_profiles.sql` adds:

- `provider_credential_pools`;
- `provider_accounts`;
- immutable `executor_resource_policies` revisions with an allocation counter;
- `provider_execution_profiles`;
- immutable binding columns on `provider_submissions`;
- a write-once profile fence on `work_items`;
- `executor_capacity_allocations`.

Historical terminal submissions may remain unbound. Migration fails closed if
an old prepared, leased, or running executor submission exists. Every new
submission must name a complete enabled profile; partial or unbound identities
are rejected by PostgreSQL.

## 3. Selection And Claim

The preparation contract is explicit:

```rust
prepare_for_lease(work_lease, execution_profile_id)
```

Preparation locks and verifies the durable job command, checks the profile's
provider and command schema, writes the work-item profile fence, and inserts
all output submissions with the full profile snapshot. Repeated preparation
must reproduce the same output, submission, profile, adapter, and command
identities.

Claim uses one PostgreSQL transaction:

1. lock and validate the enabled profile and exact policy revision;
2. select a matching prepared or expired-unstarted execution;
3. for a fresh execution, increment `allocated_count` only below the policy
   limit and insert a held allocation;
4. for a reclaim, require and reuse the existing held allocation;
5. transition the executor to leased with the next epoch;
6. commit before `start()` may grant runner launch authority.

Profile-row locking serializes allocation changes for the exact resource
policy. Deferred database constraints require `allocated_count` to equal the
number of held allocations at commit, so direct counter edits and partial
transactions fail closed.

## 4. Release

Allocation state is `held -> released` and cannot be deleted or reopened. A
release names the executor resolution decision and resolved state through a
composite foreign key.

- active runner success, failure, or uncertainty releases in the same
  transaction that projects the terminal resolution;
- an expired unstarted lease releases only with the fenced
  `executor_start_abandoned` canceled decision;
- an expired running lease remains held when no terminal runner evidence
  exists;
- late terminal evidence releases the held allocation without rewriting the
  canonical uncertain decision;
- replay of a committed terminal release is idempotent and never decrements the
  policy counter twice.

Disabling a profile prevents fresh prepare/claim allocation. It does not erase
the profile or prevent an existing running execution from attaching and
publishing terminal evidence.

## 5. Runtime Binding

`executord` requires:

- `EXECUTOR_PROFILE_KEY`;
- `EXECUTOR_CREDENTIAL_REF`;
- `EXECUTOR_CREDENTIAL_REVISION`;
- private runner, artifact, helper, executable, and credential-home paths.

It loads the profile from PostgreSQL and rejects any mismatch with the compiled
Codex generation adapter. Each immutable provider-account revision also stores
the expected SHA-256 of `auth.json`; executord verifies that digest at startup
and whenever it copies or reuses credentials in the private spool. The
credential reference therefore cannot be paired with another account's actual
auth material. Owner singleton locking includes profile ID and adapter
revision. Executor leases and the filesystem journal persist the profile ID
and adapter revision, so restart attach cannot substitute another binding.

## 6. Verification

The PostgreSQL executor suite proves:

- twenty concurrent claims at limit two produce exactly two held allocations;
- a terminal release admits exactly one waiting claim;
- expired leased reclaim keeps one allocation and one counter unit;
- reclaim takes priority over fresh submissions after a `SKIP LOCKED`
  interleaving;
- policy insertion cannot forge a nonzero counter or bypass held capacity on
  an older revision;
- migration 0013 waits for in-flight old writers before applying its drain
  gate;
- a mismatched credential home is rejected before provider launch;
- forged release and direct counter drift fail at commit;
- disabled profiles reject new claims but preserve running attach;
- running expiry without evidence retains capacity;
- late evidence and abandoned start release exactly once;
- process restart still invokes the provider exactly once;
- journal replay rejects a changed profile or adapter revision.

## 7. Remaining Activation Gates

- atomic `workerd -> awaiting_executor` handoff independent of the short worker
  lease;
- canonical executor terminal read-side and trusted receipt construction;
- one-transaction output economics plus parent work/job/idempotency reduction;
- normalized customer artifact publication and exact official response replay;
- external agentic-CLI sandbox, dedicated service identity, cgroup/mount/network
  policy, and executable image pinning;
- fake and credentialed real Codex CLI runs through the public V2 Images API.
