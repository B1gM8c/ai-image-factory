# Verified hotfix candidates

This is an explicit, default-off operator channel. It does not weaken ordinary
immutable Release checks, add a public update API, or authorize model requests.

1. Review/merge the source and require green main CI. Create a new signed,
   protected tag containing `.hotfix.`; never move an existing tag. Dispatch
   `release.yml` on that exact tag with `mode=candidate`. Its existing release
   environment approval, quality/recovery gates and both architecture builds
   still apply. The successful run attests assets but creates no Release.
2. Record the signed tag object, commit, successful run ID/attempt and exact
   `release-TARGET` artifact ID, ZIP digest/size, manifest digest and bundle
   digest. These form the root-protected candidate pin. Do not select an artifact
   by newest timestamp or accept a failed/in-progress run.
3. Obtain `scripts/verify-candidate.py` from the reviewed signed source, not an
   unverified archive. Using the host's existing trusted `gh`, run it with
   `--repo`, `--pin`, `--target`, a new root-protected `--output` directory and
   `--extract-updater`. It verifies provenance before extracting an executable.
   No service, policy, database or current-pointer change occurs in this step.
4. In an approved maintenance window, keep effective Gateway/updater Apply
   disabled and configure `AIF_UPDATE_CANDIDATE_PIN` for the updater and the
   verified bootstrap process. No active update or recovery descriptor may
   exist. Run the verified executable's `bootstrap-candidate` with the existing
   updater environment. It stages/verifies the complete tree, holds independent
   host/cluster/enqueue guards, releases BOTH host and cluster before the
   existing supervisor helper starts recovery-gate, and retains the enqueue
   fence through handoff verification. Failures attempt bounded fixed-binary
   rollback. A rollback failure requires operator recovery; it is not success.
5. `pending_owner_check=true` is not deployment acceptance. Through the existing
   authenticated owner UI, run Check and verify the successful command's exact
   candidate source/IDs/hashes. Only then enable the existing Apply policy and
   Apply that same pinned version. Drain, backup, migrations, recovery and
   verification remain the original updater state machine. A changed pin
   requires another Check. Do not manually modify database state or `current`.
6. After effect verification, record the original run's `candidate-publication`
   artifact ID/ZIP digest/size. Dispatch the same tag with `mode=promote` and its
   publication pin JSON (no architecture-specific manifest/bundle fields).
   Promotion revalidates the original certificates and publishes its exact five
   assets, without rebuilding. Compare deployed manifest/bundle hashes, verify
   the immutable Release and every asset/attestation, then retire the candidate
   pin through normal maintenance. Preserve the previous release/recovery point
   until acceptance is complete.

The isolated native rehearsal uses synthetic GitHub metadata, real systemd/PG,
and no inference. Its same-binary bootstrap/owner-Check cases do not establish
cryptographic acceptance or a second candidate application Apply. Real
candidate provenance, promotion byte equality and production health remain
separate acceptance evidence.
