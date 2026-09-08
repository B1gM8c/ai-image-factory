# Local lifecycle upgrade verification — 2026-09-08

## Scope and identity

The Blog task explicitly confirmed its local `18191` submission window was
stopped before this upgrade. Only local Factory `127.0.0.1:18491` and its
existing consumers were switched. No production deployment, git merge/push,
new image generation, cold bbox inference, quota observation or credit reset
was performed.

Source: `codex/segments-lifecycle-quota-20260908`, base
`1a1a22ffb4f64b3b8df45e74311a348aa9f4d21d`, plus the **uncommitted** lifecycle
patch in `/private/tmp/aif-segments-lifecycle-20260908.WYRjvV`.
The base SHA alone does not identify these locally built binaries.

| Binary | SHA-256 | New PID(s) |
| --- | --- | --- |
| gpt-image-2-gateway | `d0d85998560e95d90623e8146a6432bab3ab1e2208d9ebd89171202c8393f8d7` | 71590 |
| segmentd | `e2b53df03fb398779182b0689184f9aff1b43cee3617bb5d905aada0a0191710` | 71589 |
| factoryctl | `72b0a45afb9fe5d9d5d323cdbe16211d963437068f8952c1183290da7f2319dc` | one-shot |
| workerd | `edcec5c961a8c605acd173812109285a41ef840dc55b2510c3a1451944092bd3` | 71585, 71587 |
| executord | `d6e95f762da40e643faf120a7c40dcf8d754abb507dcab05b7b103bfd53fc3a1` | 71581, 71583 |
| reducerd | `7a5ca2909afc20ba711e2f04cca658575b5ff46b7fc104b1d102537f0fa2df79` | 71579 |

All seven process executable paths were checked after serial startup. The old
worktree and binaries remain untouched. The existing native Codex executable
remains `0.153.4`; it was not upgraded or invoked for inference in this window.

## Local checks

- `cargo build -p gpt-image-2-gateway --bins`, formatting and diff checks passed.
- Latest sidecar HTTP suite: 8 passed, 1 live-model test intentionally ignored.
- Latest real PostgreSQL sidecar suite: 3 passed. Cache fixture, n=25:
  P50 0.602 ms, P95 1.088 ms; not a production latency guarantee.
- Isolated real-process smoke passed migration checks, missing/stale worker
  rejection, mixed cleanup-mode rejection across both same and different
  analyzer keys, and flags-off rollback. It produced zero jobs, sidecars or
  quota refresh rows and removed its own temporary schema.
- Earlier full local library run was **not fully green**: 605 passed, 7 ignored,
  one macOS KeychainUnavailable failure reproduced on clean base `1a1a22f`.
  Strict Clippy likewise had 136 baseline errors and no added diagnostics in
  the recorded comparison. These unrelated gates are not claimed passed.

## Backup and migration

Before migration, active jobs, sidecars and executor leases were zero. Existing
provider submission and terminal reduction were terminal. All seven old PIDs
were stopped; no Factory consumers remained connected to the test database.

Private backup directory (mode 0700):
`/private/tmp/aif-local-lifecycle-upgrade-20260908.53QuNb`.
It contains a PostgreSQL custom-format public-schema dump, artifact/provider-home
archive, original runtime/config/credential archive and old PID records. Backup
files are mode 0600; dump listing and gzip integrity checks passed. Do not publish
the runtime archive because it contains credentials.

The new `factoryctl migrate` and read-only `verify-migrations` passed. Versions
129, 130 and 131 are successful. New migration file SHA-256 values:

- 0130: `52602a18ae91ed7588eb05eb5a92a49693bd375fbe2df7273569aa8f2d6afffb`
- 0131: `50f74d88108982c7d798dd99f8ba232039e6bfc43f3250ae3bc7c097d194881d`

## Live read-only acceptance

Both new flags remain explicitly **false**. The deployment hook passed with
`--source-release disabled --quota-refresh disabled`.

- `/readyz`: 200.
- `/v1/media/capabilities`: bbox, terminal-source release and analyzer pin
  supported; masks unsupported.
- `/v1/media/readiness`: 200, `status=ready`, 150000 ms heartbeat TTL,
  `source_release_enabled=false`.
- Analyzer key:
  `2b658e2f33f56acbd7ed43e18dca5e1f4acf7177fb49cb22cf26b9dbdb9fa3f3`.
- Both existing completed results remained HTTP-readable with their respective
  original credentials: Blog result 853x1844 / 4 groups, default-project result
  1920x480 / 5 groups. Reading the default-project result with the Blog key
  correctly returned 404; the two results do not share an authorization scope.
- Database remains: 1 succeeded image job, 2 completed sidecars, 2 retained
  sources, 0 quota-refresh records.
- Existing test balance unchanged: limit 80000, captured 40000, held/refunded 0
  (micro-units). No billing mutation was performed.

This proves Factory local flags-off compatibility, not a newly completed Blog
end-to-end or production acceptance. Blog owns its next local switch and at most
one separately coordinated cold bbox test using an existing image. Final client
policy is automatic first-use admission with durable reuse, never automatic
re-analysis on repeated opens or after failure/expiry.

Rollback must use the new migration-compatible binaries with feature flags off.
Do not switch a version-131 database directly back to the old version-129 binaries
or delete migration records. See `media-lifecycle-operations.md`.

## Subsequent release-candidate checks (not a production cutover)

- Full workspace tests against the real disposable PostgreSQL database passed
  with only the independently reproduced baseline macOS Keychain test explicitly
  filtered. Existing ignored live-provider tests remain ignored; this is not a
  claim of live coverage for every provider.
- Four new real-PG fault tests passed in 14.23 seconds: dedicated pool settings
  and exhaustion, advisory/table locks, statement/application deadlines and
  cancellation rollback/recovery. The original image pool settings are unchanged.
- A deferred PostgreSQL trigger held COMMIT for 6.5 seconds. The HTTP registration
  returned 503 after its store deadline, and an independent query initially saw
  no asset. After the late COMMIT the original pixels remained readable and a
  repeat upload reused the same asset ID (one row). This regression test passed
  in 6.59 seconds; it deliberately uses a test pool without the production SQL
  statement limit to reproduce an ambiguous commit acknowledgement.
- The Python media-gate suite (8 tests), release-process shell harness and gateway
  runtime-gate shell harness passed. Formatting and whitespace checks passed.
- Linux x86-64 GNU cross-compilation targeting glibc 2.35 succeeded locally.
  The initial candidate build took 2m09s; this measurement precedes final commit
  identity and is not a deployed-binary claim.

No production migration or process switch occurred during these checks. Production
was still migration 128; the candidate package must explicitly declare a minimum
schema of 128 (the packager's default of target minus one would be 130).
