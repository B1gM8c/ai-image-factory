# Phase 1E Output Economic Kernel

Status: implementation checkpoint. The schema and output settlement port are
implemented, but production paid traffic remains gated until `executord` and
the parent job reducer are connected.

This document narrows the target architecture in
`2026-ai-image-factory-target-architecture.md`. It does not replace it.

## 1. Decision

The billing unit is one accepted `job_output`, not one HTTP request, work item,
worker attempt, provider process, or parent job. Final durable admission freezes
the following identity set in one PostgreSQL transaction:

```text
job
  -> price quote
  -> output[0..n)
       -> monetary hold
       -> provider submission
       -> provider receipt
       -> meter fact
       -> rating
       -> ledger transactions and postings
```

The caller supplies the image command and provider evidence. It never supplies
the selected price, captured amount, rating, ledger account, or posting.

## 2. Amount And Price Rules

- All monetary values use signed PostgreSQL `BIGINT` integer micros.
- Currencies are three uppercase ASCII characters.
- Published price fields and accepted quotes are immutable.
- A quote freezes success, failure, and proven-no-effect prices plus the maximum
  hold for all requested outputs.
- Quote multiplication is checked before conversion to `BIGINT`; Rust admission
  also uses `checked_mul`.
- Changing the active price never reprices an accepted job.
- Zero prices still create a quote, hold, receipt, meter, and rating. A zero
  customer charge creates no zero-value ledger posting.
- Provider cost and customer charge are separate ledger transactions.

The migration seeds a wildcard zero-price `platform-beta-default` version so
the current Codex API remains backward compatible. A paid route must publish a
more-specific active price and provision tenant credit before acceptance.

## 3. Durable State

### Admission contract versions

- Version 1 is the legacy job-level execution and settlement path.
- Version 2 owns outputs at admission and requires a frozen quote and one hold
  for every output.

`prepare_and_handoff` accepts only version 2 jobs. It fails closed if outputs
are missing, non-contiguous, or partially attached, and creates submission and
executor identities only for admission-owned output IDs. LegacyV1 remains on
the isolated inline worker path and cannot enter the executor handoff state.

### Economic facts

`price_quotes`, `provider_receipts`, `economic_metering_events`, `rated_usage`,
`ledger_accounts`, `ledger_transactions`, and `ledger_postings` reject update,
delete, and truncate operations at the database boundary. Corrections require
new compensating facts.

`output_holds` and `billing_accounts` are controlled state, not append-only
facts. A final output atomically converts its hold into captured and released
micros. An uncertain output retains the full hold and has no rating or customer
ledger transaction.

## 4. Settlement Authority

The provider submission store owns provider execution evidence only. A success
is economically eligible only when the submission is terminal `succeeded` and
has a durable executor result manifest. A process exit code is insufficient.

`PostgresEconomicSettlementStore` then:

1. validates the receipt and loads the immutable submission identity;
2. takes the tenant budget advisory lock;
3. locks parent job, output and hold, then provider submission;
4. compares terminal provider evidence;
5. deduplicates by submission and semantic receipt hash;
6. appends receipt and meter facts;
7. rates from the locked quote;
8. captures/releases the hold and updates the billing account;
9. writes independent provider-cost and customer-charge ledger transactions;
10. terminalizes the output and commits.

Identical replay returns the committed IDs. A replay with different evidence is
a conflict. This makes ordinary lost-COMMIT acknowledgement replay idempotent;
connection-level fault injection remains a rollout gate.

## 5. Ledger Invariants

Each transaction has one currency and at least two non-zero postings. Composite
foreign keys prevent a posting from changing the transaction or account
currency. Deferred constraint triggers run at COMMIT and require:

```text
posting count >= 2
SUM(amount_micros::NUMERIC) = 0
```

Both the parent transaction and every posting install a deferred trigger. An
empty transaction therefore cannot bypass validation.

Current posting conventions are:

```text
customer charge:  + tenant receivable, - platform revenue
provider cost:    + platform expense,   - provider payable
```

Account keys include currency so independent currencies cannot alias one
account identity.

## 6. Lock Order

New output economics uses this order:

```text
budget advisory lock
-> parent job
-> output and hold
-> provider submission
-> immutable semantic facts
-> ledger accounts and postings
```

Provider outcome recording locks executor execution and submission in its own
transaction and never waits for job, output, hold, or ledger rows. The economic
reducer runs only after provider outcome commit, so this order does not form a
cycle with executor fencing.

Admission serializes session ownership first, validates the job, freezes quote,
outputs and holds, then publishes payload/work/idempotency acceptance in the
same transaction.

## 7. Mixed-Version Safety

Migration verification now rejects a database schema newer than the running
binary. This prevents an older process from silently writing terminal jobs
without the economic facts required by a newer schema.

Version 1 jobs remain readable and executable. Version 2 paid traffic must not
be enabled on legacy `workerd`: its current settlement is job-scoped and does
not consume provider submissions. The rollout order is:

1. deploy migration and version-aware executor preparation;
2. deploy `executord` and durable start-or-attach runner;
3. connect provider receipt settlement and the parent reducer;
4. enable version 2 for one zero-price Codex route;
5. pass crash and API replay gates;
6. provision paid prices and credit only after the full gate passes.

## 8. Verification In This Checkpoint

Real PostgreSQL tests cover:

- accept/replay creates one quote, contiguous output set, and one hold per output;
- insufficient credit rolls back session attachment and all economic rows;
- executor preparation preserves admission-owned output IDs;
- identical receipt replay creates one receipt, meter, rating, and charge;
- non-zero customer rating creates exactly two balanced postings;
- uncertain provider evidence keeps the complete hold and creates no rating;
- empty ledger transactions fail at COMMIT;
- ledger facts reject mutation;
- a newer database migration fails binary verification.

## 9. Remaining Gates

This checkpoint does not yet claim production paid readiness. The following
remain mandatory:

- parent job reducer with succeeded, failed, partial, and uncertain aggregates;
- quota slices committed or released per output;
- resolution of uncertain output through an audited replacement allocation;
- persistent `executord` and runner `start_or_attach` process boundary;
- provider receipt creation inside every adapter, including Codex, Jimeng and
  Grok;
- concurrent settle-versus-reconcile and multi-output deadlock tests;
- COMMIT-before-ACK connection fault injection;
- restricted production database roles that cannot disable triggers;
- real Images API replay through gateway, PostgreSQL, executord, runner, Codex,
  artifact publication, economic reducer, parent reducer and outbox.
