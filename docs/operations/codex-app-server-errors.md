# Codex image error notifications

The image adapter interprets the `rust-v0.145.0` app-server protocol. This
reference is a protocol contract, not a claim about the installed native CLI
version; record the deployed release and actual executable identity separately.

## Recovery boundary

An error notification cannot establish a thread or turn identity. It must match
the already bound invocation. Only an explicit `willRetry=true` with a recognized
transient structured error may keep that same invocation open. Authentication,
non-transient HTTP, quota, request and content-policy evidence veto recovery;
missing, malformed, unknown or contradictory data fails closed.

Continuing the stream does not resend a request, create another turn, restart
the provider or refresh authentication. The original timeout and capture budget
remain cumulative. The app-server itself owns any protocol-declared recovery.
This does not prove the upstream performed only one billable operation.

Success still requires the matching successful terminal turn, exactly one
authorized native image-tool completion and a valid output artifact. Stream
loss, cancellation, process exit or a late event cannot manufacture success.
Content-policy evidence remains terminal and cannot be erased by a later
generic error. A generic image-item failure must not discard a later recognized
structured terminal reason.

## Safe diagnostics

Retain only bounded, allowlisted reason kinds, HTTP status, recovery flag/count,
protocol phase and invocation correlation metadata. A fixed `recovery_blocker`
explains why a claimed recovery was refused; `metadata_rejected` denotes an
adapter validation decision, not a confirmed upstream root cause. Raw messages,
additional details, prompts, credentials and arbitrary upstream strings are not persisted
or logged; existing message/code digests are not reversible error text.

The invocation's protocol contract and Factory crate version are not the
installed Codex CLI version. Use the request/execution graph and the deployed
release record to establish runtime provenance. New readers accept the previous
diagnostic shape with absent optional fields; old strict readers do not accept
new fields. Do not describe this as bidirectional diagnostic compatibility.

## Verification and rollout

Use synthetic parser/subprocess fixtures and the explicitly isolated local
PostgreSQL process-smoke suite to test recovery, terminal/policy/auth failures,
identity mismatch, deadline/capture limits, exit/cancel/late events, single
launch/turn and single settlement across idempotent reads/replay.

Offline success does not establish a customer's historical failure cause or
prove a new production generation. Follow the signed candidate, normal owner
Check/Apply, backup and full runtime-verification procedure. Do not replay
customer orders, add a paid canary or promote a Release without authorization.
