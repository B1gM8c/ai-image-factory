# Phase 1H: Atomic Executor Handoff

Status: implemented for internal V2 generation work. Public Images V2 remains
gated on terminal reduction, customer artifact publication, and end-to-end API
activation tests.

## 1. Ownership Rule

Exactly one subsystem owns active work:

```text
ready
  -> leased / claimed                 workerd owns a short lease
  -> awaiting_executor / handed_off   PostgreSQL executor queue owns the work
  -> executor prepared/leased/running executord owns an independent lease
  -> executor terminal                reducer owns customer-facing completion
```

`prepare_and_handoff(work_lease, execution_profile_id)` is the only V2
transition from worker ownership to executor ownership. One PostgreSQL
transaction:

1. locks job, work, and attempt in canonical order;
2. validates the exact live worker identity, epoch, command, and V2 contract;
3. freezes the enabled execution profile;
4. creates or verifies every output submission, executor execution, and
   current-attempt attachment;
5. changes the attempt to `handed_off`;
6. changes work to `awaiting_executor` and clears worker owner and expiry;
7. commits all identities and projections together.

There is no state in which executord can claim an uncommitted submission, and
no committed handoff remains dependent on the former worker deadline.

## 2. Database Enforcement

Migration `0014_executor_handoff.sql` adds:

- `work_items.state = awaiting_executor`;
- `job_attempts.state = handed_off`;
- immutable `handed_off_at_ms` columns on both rows;
- one-way state-transition triggers;
- a deferred handoff-completeness trigger;
- a drain gate for active rows created by pre-handoff binaries.

At commit, the deferred trigger requires a V2 job, matching work and attempt
timestamps, contiguous admission-owned outputs, and exactly one prepared
submission, prepared executor execution, and attachment per output. Direct SQL
cannot publish a partial executor queue item.

## 3. Replay And Fencing

An acknowledgement may be lost after commit. Repeating the operation with the
same work item, execution ID, worker ID, epoch, command, and profile returns the
same identities without rewriting the handoff timestamp. This replay remains
valid if the bound profile is later disabled because disabling affects new
selection, not immutable recovery.

A different worker, epoch, command, or profile is rejected. Handed-off work
cannot return to `leased` or `running`, regain worker lease fields, or be
selected by worker reconciliation.

Executord claim and start require the exact
`awaiting_executor/handed_off/profile/attachment` parent. They do not inspect a
worker lease deadline. Executor lease expiry and capacity remain independent
and retain their existing reclaim and terminal-resolution rules.

## 4. Runtime Wiring

Workerd exposes only the narrow `ExecutorHandoffStore` port. It does not depend
on executor claim, heartbeat, launch, evidence, or terminal APIs. In explicit
`WORKER_EXECUTION_MODE=executor-handoff`, workerd loads and validates the same
`EXECUTOR_PROFILE_KEY` used by executord. The handoff-only process does not
construct or receive a generator, settlement store, artifact store, input
store, Codex home, or provider credential.

Ready-work claims are partitioned inside the locking scheduler query by
`economics_contract_version`. A handoff worker can claim only V2 work, while an
inline worker can claim only LegacyV1 work; contract detection never happens
after ownership has already moved to the wrong runtime.

- LegacyV1 generation and edits keep the existing inline path during rollout.
- V2 generation invokes only `prepare_and_handoff`; `ImageGenerator` is not
  called.
- V2 generation without a valid profile fails closed.
- V2 edit handoff remains closed until an edit-capable adapter and executor
  profile exist.

## 5. Verified Failure Boundaries

- 24 concurrent handoff replays return one stable identity set.
- A deferred commit failure leaves work `leased`, attempt `claimed`, profile
  unbound, and zero submissions, executions, or attachments; exact retry then
  succeeds.
- A committed handoff replays after profile disable and rejects another profile
  or epoch.
- Worker reconciliation ignores executor-owned work after the old lease time.
- 20 concurrent executor claims produce one winner per output.
- Executord starts after handoff with no worker owner or expiry present.
- A production Workerd instance hands off V2 work without one inline provider
  invocation.
- A V2 worker skips an older queued LegacyV1 job, and the LegacyV1 worker still
  claims that job without starvation.

## 6. Remaining Boundary

Executor terminal evidence now enters a canonical leased reduction queue through
migration 0015. The next slice must publish the private executor artifact into a
deterministic customer namespace and atomically reduce output economics plus
parent work, job, idempotency, and outbox projections. Public V2 routing stays
disabled until that reducer and the fake plus credentialed Codex API smokes pass.
