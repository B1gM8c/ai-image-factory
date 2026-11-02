# Poll workspace restart compatibility

Normal polling and capacity reconciliation each own an exclusive attempt workspace.
For `PROVIDER_POLLER_WORKSPACE_ROOT=/var/lib/ai-image-factory/remote-runners/PROFILE/poll`,
the capacity workspace is the sibling `poll.capacity-reconciliation`, not a child
of `poll`. Each root retains its own lock, exact attempt prefix, private ownership,
and startup cleanup. Both roots must be separate from account homes and artifacts.
The supplied systemd units already permit these siblings beneath
`/var/lib/ai-image-factory`; no sandbox expansion is needed.

## Existing nested layout

Older pollers created `poll/capacity-reconciliation` after acquiring `poll`.
That unknown child prevents the next normal poll workspace acquisition. The new
binary deliberately does not migrate, ignore, or delete that child automatically.

Before upgrading an affected host:

1. Use the normal drained maintenance flow. Stop every poller owning an affected
   root, and prevent automatic restarts until the new writers have switched.
2. Acquire the existing normal and nested workspace locks without invoking startup
   cleanup. Verify the roots are real private directories with expected owner and
   device, and that the nested directory contains **only** its regular, same-owner,
   mode-0600, single-link, empty `.cli-attempt-workspace.lock` file. Unknown entries,
   active attempts, changed identities, or lock contention require investigation;
   do not delete them or relax the workspace guard.
3. Move the verified nested directory intact into the maintenance evidence backup
   outside all attempt roots, preserving its metadata and recording the move.
   Do not move it into, or reuse it as, the new sibling workspace.
4. Keep the old poller stopped. A temporary restart of the old binary recreates
   the bad nested layout and requires this preflight again before final activation.
5. Continue normal activation of the new binary, then verify both workspace locks,
   stable poller PIDs, and task polling. A subsequent normal restart must succeed.

This migration does not resubmit a task, alter a provider result, change billing,
or waive unknown task capacity. It must not change any frozen task runtime binding.
