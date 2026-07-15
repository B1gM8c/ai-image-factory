# Phase 1G: Execution Binding And Durable Capacity

Status: implemented on the internal executor path. Migration 0014 and the V2
workerd path now commit submission preparation and executor ownership handoff
atomically. Public Images V2 traffic remains disabled until the canonical
reducer, customer artifact publication, and real API smoke gates are complete.

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
prepare_and_handoff(work_lease, execution_profile_id)
```

The V2-only operation locks and verifies the durable job command, checks the
profile's provider and command schema, writes the work-item profile fence, and
inserts all output submissions with the full profile snapshot. In that same
transaction it moves the attempt from `claimed` to `handed_off`, moves work
from `leased` to `awaiting_executor`, and clears worker lease ownership.
Repeated calls for the exact attempt and profile reproduce the same output,
submission, profile, adapter, and command identities, including after the
bound profile is disabled.

Migration `0014_executor_handoff.sql` adds the handoff states and immutable
timestamps. A deferred constraint trigger rejects commit unless every
admission-owned output has exactly one bound submission, executor execution,
and current-attempt attachment. The migration itself requires old active
executor submissions to be drained.

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

`workerd` enables V2 generation handoff only with
`WORKER_EXECUTION_MODE=executor-handoff` and the same
`EXECUTOR_PROFILE_KEY` used by executord. It validates the database profile
before polling. This process mode does not construct a generator, settlement
store, artifact store, or input store and does not require
`GATEWAY_CODEX_HOME`. A V2 generation never falls back to inline Codex
execution. The default `legacy-inline` mode retains the existing LegacyV1 path
during migration.

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
- injected terminal projection failure rolls back observation, decision,
  projection, and capacity release together, then exact retry succeeds;
- terminal recording does not depend on a second post-run heartbeat;
- process restart still invokes the provider exactly once;
- journal replay rejects a changed profile or adapter revision.
- handoff commit failure rolls back submissions, executions, attachments,
  profile binding, attempt state, and work state together;
- twenty concurrent claims see only committed handoffs and retain one winner
  per submission after the former worker lease deadline;
- worker reconciliation ignores `awaiting_executor` work;
- the production workerd V2 branch reaches `awaiting_executor/handed_off`
  without invoking `ImageGenerator`.

## 7. Remaining Activation Gates

- trusted receipt construction from the implemented canonical terminal queue;
- one-transaction output economics plus parent work/job/idempotency reduction;
- normalized customer artifact publication and exact official response replay;
- external agentic-CLI sandbox, dedicated service identity, cgroup/mount/network
  policy, and executable image pinning;
- fake and credentialed real Codex CLI runs through the public V2 Images API.
