# Phase 1B Durable Scheduler Policy

This slice adds the scheduling policy boundary and persists its ordering state
without moving provider execution out of the gateway yet. The policy crate has
no HTTP, SQL, provider, or runtime dependencies, so the same rules can later be
used by `schedulerd`, `workerd`, and deterministic in-memory tests.

## Policy

Each ready work item carries:

- a scheduling scope, initially the tenant;
- a positive scope weight;
- a bounded priority class from `LOW` through `URGENT`;
- an estimated cost, initially the requested image count;
- a fixed-point virtual finish tag.

Admission serializes the scope row in PostgreSQL, advances its next finish tag
by `cost / weight`, and stores the resulting tag on the work item. Claiming
uses the effective finish tag, waiting-time aging, bounded priority, and stable
creation/UUID tie breakers. `FOR UPDATE SKIP LOCKED` remains only the atomic
claim mechanism; the policy determines order before the lock is taken.

The in-memory admission implementation uses the same fixed-point calculation.
This keeps local tests useful without treating process-local state as the
production scheduler.

## Invariants

- A schedule scope has one serialized next-finish counter.
- A work item receives its finish tag in the same transaction that attaches it.
- Weight and priority are validated before a claimable work item is created.
- A canceled or failed attach cannot create executable work.
- Claiming remains fenced by work ID, execution ID, lease epoch, owner, and
  unexpired database time.
- Aging can move an old low-priority item ahead of newer urgent work; priority
  never becomes an unbounded starvation mechanism.
- Every ordering tie has a deterministic final key.

## Current Limits

The first durable policy scopes only by tenant and uses a fixed default weight
and aging configuration in the gateway. Project, provider pool, account,
execution class, queued cost backpressure, and dynamic policy snapshots remain
the next scheduler release. They must be introduced as admission-owned policy
data rather than caller-controlled request fields.

The gateway still owns synchronous Codex execution in this phase. Moving claim
and execution into `workerd`, persisting artifacts, and reconciling expired
provider effects remain separate changes so queue correctness is not confused
with provider side-effect recovery.
