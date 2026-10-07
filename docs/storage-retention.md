# Bounded deployment storage

## Successful execution copies

Set `RECONCILER_RUNNER_ROOT=/var/lib/ai-image-factory/executor-runners`
in the reconciler's environment to enable duplicate `output.bin` reclamation.
This is the parent of private profile directories, not a single profile root.
The parent must be owned by root or the service user and not group/world writable;
each child must be private and owned by the service user. At most 128 profile
directories are inspected, once per minute, with at most 100 execution candidates.
Profile names are not inferred from database keys: profiles may share a directory.

Jobs, outputs, submissions and executions must all have succeeded, the formal
artifact retention must be `deleted`, and execution completion must be more than
24 hours old. Manifest/authority, spool identity, terminal result and file size
must agree; the runner lock must be idle. Only `output.bin` is unlinked. Failed,
uncertain, running or ambiguous work is retained. No database records, billing,
formal media or task markers are changed. A keyset cursor prevents early retained
candidates from starving later ones. This uses the existing reconciler, with no
new service or migration.

## Release and backup copies

The supported updater invokes `ops/hooks/retain-storage` only after a successful
apply has completed, the protected recovery descriptor is removed and the
`verified` journal entry is durable, while still holding its advisory lock.
Failure is logged but never rolls back an already successful deployment.

Enable `AIF_UPDATE_RETENTION_HOOK=/usr/libexec/ai-image-factory/hooks/retain-storage`
in updater configuration only after installing the hook and upgrading the fixed
updater binary from the same verified signed release with `deploy/upgrade-updater`.
An application-only release switch does not replace that fixed updater binary.

The hook requires explicit absolute `AIF_RELEASE_ROOT`, `AIF_BACKUP_ROOT`,
`AIF_UPDATE_JOURNAL_ROOT`, `AIF_UPDATE_RELEASE_DIR`,
`AIF_UPDATE_PREVIOUS_RELEASE` and `AIF_UPDATE_BACKUP_TOKEN` paths. These must
come from the updater's verified apply context, not HTTP input. It emits a JSON
object with `removed_releases` and `removed_backups` counts. Do not run it
manually concurrently with updater or recovery operations.

Retention keeps current, previous and the three latest journal-verified release
versions (their union), plus any release referenced by process executable, cwd,
open file, memory map or systemd/site configuration. Associated backups are
also protected. Only journal-proven successful applies older than seven days
are eligible. Backup command/version/commit metadata must match the successful
apply; legacy or unmatched backups are not automatically removed. This is
conservative retention, not an unconditional disk-size cap. A known recovery
point matching the just-completed apply is always retained with its previous
release.

Any pending recovery descriptor, malformed journal, inaccessible reference
scan, changed current pointer, link/special file/mount in a deletion candidate
or unsupported safe recursive deletion aborts the plan. No database, accounting,
formal media, credentials, executor spool or uncertain task is deleted by this
hook. These resources have separate lifecycle requirements.

Removed release copies can be downloaded again from their immutable release;
removed database/artifact backups are permanently deleted. This policy is not
a replacement for independent disaster-recovery backups. Automatic cleanup
runs on future successful deployments, not on generation requests or a timer.
