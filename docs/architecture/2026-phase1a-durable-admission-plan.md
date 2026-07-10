# Phase 1A Durable Admission Implementation Plan

> **For agentic workers:** implement each task with tests first, focused commits,
> and an adversarial review before changing the execution owner.

**Goal:** Put every OpenAI generation request behind a durable, fenced
PostgreSQL admission and work protocol before moving Codex execution into
`workerd` and `executord`.

**Architecture:** This is an additive expand/contract slice inside the current
backend crate. The gateway remains the temporary execution owner, but it must
claim and settle the same durable work item that a future worker will use. No
new provider is allowed during this phase.

## Scope

1. Add immutable idempotency identity, admission session, replayable generation
   command, work item, attempt, job event, and outbox tables.
2. Add a narrow `AdmissionStore` port with in-memory and PostgreSQL
   implementations.
3. Canonicalize and hash the validated generation command. Support the
   `Idempotency-Key` header as a documented extension.
4. Claim admission before quota reservation, attach the existing reserved job,
   claim one fenced work lease, execute the current generator, and settle the
   work item before committing or releasing quota.
5. Preserve the current synchronous response and error body when no
   idempotency key is supplied.

## Invariants

- `(project_id, api_profile, operation, key_digest)` creates at most one job.
- Reusing a key with a different request hash returns conflict and creates no
  quota reservation or provider execution.
- A concurrent same-hash request returns `idempotency_in_progress` and never
  invokes Codex.
- An accepted or terminal same-hash request never creates another job. Until
  durable artifact projection is implemented, it returns an explicit
  `idempotency_result_unavailable` response instead of re-executing.
- Only the admission owner token may attach a job.
- Work settlement requires the current lease epoch; stale workers cannot
  succeed or fail work.
- Command JSON stores the validated model, prompt, `n`, size, quality, format,
  background, source API profile, schema version, and request hash.
- Admission, work, and terminal transitions append job events and transactional
  outbox rows with semantic uniqueness keys.
- Image bytes and base64 responses are never stored in PostgreSQL.

## Task 1: Additive schema and store contract

Create `0002_durable_admission.sql` and `src/admission/` modules. Start with
integration tests for concurrent idempotency claims, hash conflicts, owner-only
attachment, one lease winner, stale epoch rejection, and terminal outbox
deduplication.

## Task 2: Generation command canonicalization

Create a versioned serializable generation command independent of Axum DTOs.
Hash canonical JSON with SHA-256 and add unit tests proving field-order
independence at the request boundary and field-sensitive hashes.

## Task 3: Wire the production composition

Add a router composition overload that accepts `AdmissionStore`; existing test
builders keep an in-memory implementation. The production binary uses the same
shared PostgreSQL pool for API keys, quota, and admission.

## Task 4: Wire generations without changing the response

Read and validate `Idempotency-Key`, claim admission, reserve quota, attach the
job, claim the inline lease, execute Codex, and fenced-settle work. Any failure
before provider execution aborts admission and releases quota where applicable.
Edits remain on the Phase 0 path until multipart input manifests are available.

## Verification Gate

- Existing OpenAI gateway tests remain green.
- Real PostgreSQL tests prove 100 concurrent identical keys create one job and
  one executable work item.
- Different bodies under one key return conflict.
- A stale lease cannot settle work.
- Production fake-Codex process smoke traverses the new tables.
- The real Codex smoke still returns a decodable image.
- Full workspace tests, Clippy `-D warnings`, admin typecheck, secret scan, and
  independent correctness/security reviews pass.

## Explicit Deferrals

- Durable artifact bytes and replay projection move to object storage in Phase
  1B; Phase 1A never re-executes an accepted idempotent job.
- `workerd`, persistent `executord`, crash attachment, and hostile-prompt
  credential isolation are Phase 1B and remain production hard gates.
- Multipart edit idempotency waits for durable input manifests.
- Price quotes, ledger postings, and provider receipts are added before the
  Phase 1 hard gate, not faked in this slice.
