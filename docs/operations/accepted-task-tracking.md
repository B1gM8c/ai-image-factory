# Accepted Dreamina task tracking

Schema 167 separates accepted-task tracking from the frozen submit deadline. A strict accepted, unleased `provider_waiting` operation receives an append-only authority bound to its original submission, execution, account, remote operation ID, acceptance time, and submit deadline. No new submit or financial authorization is created. The existing 24-hour budget evidence is retained unchanged.

SQL uses `i64::MAX` only as an internal no-task-lifetime-deadline value. Only newly granted unlimited tracking leases use a separate sealed attempt deadline: the smaller of the original provider timeout and five minutes from the claim. Legacy bounded tasks and other providers keep their existing total deadline and provider timeout. The single-output, additional-image, and native-batch orchestrators share that bounded attempt budget. A timed-out attempt persists normal waiting evidence with the original owner/epoch CAS, releases that lease, and schedules another query of the same remote operation. An expired or replaced lease cannot publish or release a successor's claim. Submit-time expiry and unaccepted/unknown tasks retain their existing rules.

## Deployment and rollback

1. Close admission and use the normal drained update flow. Stop **all** old submit, poll, callback/terminal, and reconciliation writers before migration; stopping the HTTP gateway alone is insufficient.
2. Confirm no Dreamina `provider_waiting` task has a poll lease. Migration 167 refuses to proceed if any such lease remains; do not clear it with SQL or change immutable evidence to pass this check. Let the old runtime finish its bounded work before stopping it, or investigate a stuck lease.
3. Migrate and install the complete new runtime, then verify every writer's binary identity before reopening admission. Old binaries must never consume the new tracking sentinel. No mixed-version rollout and no rollback to a pre-167 runtime against a migrated database.
4. Backfill includes only strictly eligible waiting tasks without leases. Existing quarantines, financial terminal states, capacity waivers, operator-supplemented jobs, and preexisting partially published native batches stay untouched. Already reclaimed or actively deleting image stages are not revived. The normal released-poll transition can grant tracking at a later unleased boundary; held seals are never changed.

Additional-image staging and result-set cleanup use a finite observation-time retention clock for tracked tasks. GC locks the same task and excludes still-waiting tracking authorities, including old stage intents migrated across their original 24-hour cleanup date. After a definite terminal outcome, the existing customer-retention or orphan-cleanup rules apply; the tracking sentinel is never added to retention durations.

`accepted_tracking_delays` lists still-waiting tracked tasks older than 24 hours using safe IDs and timestamps. This is a diagnostic view, not an implemented notification transport. Its threshold never manufactures an upstream failure, refunds a hold, or stops tracking. Upstream definitive failure/cancellation, mismatched identity, corrupt results, and settlement continue through existing evidence checks.

Acceptance requires disposable PostgreSQL tests advancing only the fixture schema clock beyond 24 hours, same-ID reclaims and heartbeat, bounded timeout/reschedule, late publication and exactly-once settlement, legacy deadline behavior, migration lease refusal, and immutable submit/authority checks. No paid generation is needed.
