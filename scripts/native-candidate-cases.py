"""Candidate-only cases called inside the disposable native recovery rehearsal.

GitHub provenance is an explicit fixture, never cryptographic acceptance.
The updater, PG locks, systemd helper and owner Check are real.
"""
import json
import os
from pathlib import Path
import time
import zipfile


def owner_check_enqueue(h, password):
    # Fault cases outlive the default five-minute access token. Authenticate
    # normally for each command; never extend TTL or retry an unauthorized write.
    return h.enqueue(h.owner_login(password), 'check')


def exercise(h, args, candidate, updater, policy, owner_env, password, output, *, different_bytes=False):
    h.require(os.environ.get('GITHUB_ACTIONS') == 'true'
              and os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted'
              and Path('/proc/1/comm').read_text().strip() == 'systemd',
              'candidate cases require the isolated native rehearsal')
    fixture = h.STATE / 'updater/fixture'
    pin_path = h.CONFIG / 'candidate-pin.json'
    github = h.LIB / 'ci-gh'
    helper = h.LIB / 'upgrade-updater'
    original_github, original_helper = github.read_bytes(), helper.read_bytes()
    original_updater = dict(updater)
    old_fixed = h.digest(h.LIB / 'updated')
    # The early case uses the already verified bundle before application Apply.
    # Later cases reinstall its bytes to exercise handoff and candidate Check.
    archive = fixture / 'candidate-actions.zip'
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_STORED) as zipped:
        for source in (args.candidate_manifest, args.candidate_bundle):
            zipped.write(source, source.name)
    pin = dict(version=candidate['release_version'], commit_sha=candidate['commit_sha'],
               tag_object_sha='d' * 40, run_id=123, run_attempt=1, artifact_id=456,
               artifact_sha256=h.digest(archive), artifact_bytes=archive.stat().st_size,
               manifest_sha256=h.digest(args.candidate_manifest),
               bundle_sha256=h.digest(args.candidate_bundle))
    h.write(pin_path, json.dumps(pin))
    h.write(fixture / 'native-helper', original_helper.decode(), 0o755)
    h.write(github, '''#!/usr/bin/python3
import json, pathlib, sys
r=pathlib.Path('/var/lib/ai-image-factory/updater/fixture')
p=json.loads(pathlib.Path('/etc/ai-image-factory/candidate-pin.json').read_text())
a=sys.argv[1:]; repo='fixture/native'; base='repos/'+repo
with (r/'gh-boundary.jsonl').open('a') as f: f.write(json.dumps(a)+'\\n')
if a==['api',base+'/git/ref/tags/'+p['version']]:
 print(json.dumps({'object':{'sha':p['tag_object_sha'],'type':'tag'}}))
elif a==['api',base+'/git/tags/'+p['tag_object_sha']]:
 print(json.dumps({'verification':{'verified':True,'reason':'valid'},'object':{'type':'commit','sha':p['commit_sha']},'tag':p['version']}))
elif a==['api',base+'/actions/runs/123']:
 print(json.dumps(dict(id=123,run_attempt=1,head_sha=p['commit_sha'],head_branch=p['version'],repository={'full_name':repo},head_repository={'full_name':repo},path='.github/workflows/release.yml',event='workflow_dispatch',status='completed',conclusion='success')))
elif a==['api',base+'/actions/artifacts/456']:
 print(json.dumps(dict(id=456,name='release-x86_64-unknown-linux-gnu',expired=False,size_in_bytes=p['artifact_bytes'],digest='sha256:'+p['artifact_sha256'],workflow_run={'id':123,'head_sha':p['commit_sha']})))
elif a==['api',base+'/actions/artifacts/456/zip']:
 with (r/'candidate-actions.zip').open('rb') as f:
  import shutil
  shutil.copyfileobj(f,sys.stdout.buffer)
elif a[:2]==['attestation','verify']:
 assert a[a.index('--repo')+1]==repo and '--deny-self-hosted-runners' in a
 assert a[a.index('--source-digest')+1]==p['commit_sha']
 assert a[a.index('--source-ref')+1]=='refs/tags/'+p['version']
 print(json.dumps([{'verificationResult':{'signature':{'certificate':dict(runInvocationURI='https://github.com/'+repo+'/actions/runs/123/attempts/1',buildSignerURI='https://github.com/'+repo+'/.github/workflows/release.yml@refs/tags/'+p['version'],buildSignerDigest=p['commit_sha'],sourceRepositoryDigest=p['commit_sha'],sourceRepositoryRef='refs/tags/'+p['version'],runnerEnvironment='github-hosted',buildTrigger='workflow_dispatch',issuer='https://token.actions.githubusercontent.com')}}}]))
else: raise SystemExit('unexpected candidate GitHub fixture call')
''', 0o755)
    updater['AIF_UPDATE_CANDIDATE_PIN'] = str(pin_path)
    h.env_file(h.CONFIG / 'updater.env', updater)
    if not different_bytes:
        h.run(['systemctl', 'restart', h.PREFIX + 'updater.service'])
    environment = dict(os.environ) | updater | policy
    binary = h.ROOT / 'current/bin/updated'
    if different_bytes:
        staged = h.ROOT / 'releases' / candidate['release_version']
        h.unpack(args.candidate_bundle, staged)
        binary = staged / 'bin/updated'
        h.require(h.digest(binary) != old_fixed, 'rollback case requires genuinely different executables')
    receipts = {}

    def bootstrap():
        # The daemon legitimately takes the nonblocking host mutex each poll.
        # Only retry this pre-mutation refusal, never a helper/verification error.
        deadline = time.monotonic() + 30
        while True:
            result = h.run([binary, 'bootstrap-candidate'], env=environment, timeout=270, check=False)
            if result.returncode == 0 or 'another updater owns the host lock' not in result.stderr:
                return result
            h.require(time.monotonic() < deadline, 'bootstrap never acquired idle host mutex')
            time.sleep(0.15)

    def snapshot():
        units = h.run(['systemctl', 'list-units', '--all', '--plain', '--full',
                       '--no-legend', '--no-pager', '--type=service', 'ai-image-factory*']).stdout
        names = sorted(line.split()[0] for line in units.splitlines() if line.split()
                       and not line.split()[0].startswith((h.PREFIX + 'updater', h.PREFIX + 'recovery-')))
        return dict(current=str((h.ROOT / 'current').resolve()),
                    schema=h.sql(owner_env, 'SELECT max(version) FROM _sqlx_migrations;'),
                    business=h.run(['systemctl', 'show', '--no-pager',
                                    '--property=Id,MainPID,InvocationID,NRestarts,ActiveState,SubState', *names]).stdout,
                    artifacts=h.artifact_snapshot())

    def owner_check():
        command = owner_check_enqueue(h, password)
        h.wait_command(owner_env, command, {'succeeded'})
        progress = json.loads(h.sql(owner_env,
            "SELECT progress::text FROM platform_update_commands WHERE command_id='" + command + "';"))
        h.require(progress.get('source') == 'actions_candidate' and progress.get('immutable') is False,
                  'owner Check did not return the candidate source receipt')
        h.require(progress.get('latest_version') == pin['version'], 'owner Check version mismatch')
        for field in ('run_id', 'run_attempt', 'artifact_id', 'artifact_sha256', 'artifact_bytes',
                      'commit_sha', 'tag_object_sha', 'manifest_sha256', 'bundle_sha256'):
            h.require(progress.get(field) == pin[field], 'owner Check candidate receipt mismatch: ' + field)
        return command

    baseline = snapshot()
    try:
        if different_bytes:
            h.write(fixture / 'after-helper-once', 'inject after real replacement')
            h.write(helper, '''#!/usr/bin/python3
import hashlib, json, os, pathlib, subprocess, sys
r=pathlib.Path('/var/lib/ai-image-factory/updater/fixture'); marker=r/'after-helper-once'
if marker.exists():
 marker.unlink()
 subprocess.run([str(r/'native-helper'),*sys.argv[1:]],check=True)
 pid=subprocess.check_output(['/usr/bin/systemctl','show','ai-image-factory-updater.service','--value','--property=MainPID'],text=True).strip()
 def digest(path):
  with open(path,'rb') as f: return hashlib.file_digest(f,'sha256').hexdigest()
 evidence=dict(pid=int(pid),fixed_sha256=digest('/usr/libexec/ai-image-factory/updated'),process_sha256=digest('/proc/'+pid+'/exe'))
 (r/'after-helper-observed.json').write_text(json.dumps(evidence))
 raise SystemExit(42)
os.execv(str(r/'native-helper'),[str(r/'native-helper'),*sys.argv[1:]])
''', 0o755)
            failed = bootstrap()
            h.require(failed.returncode != 0 and '42' in failed.stderr,
                      'after-helper injection did not reach the intended failure; exit='
                      + str(failed.returncode) + '; stderr=' + h.sanitized(failed.stderr)[-4096:])
            observed = json.loads((fixture / 'after-helper-observed.json').read_text())
            candidate_digest = h.digest(binary)
            h.require(observed['fixed_sha256'] == candidate_digest
                      and observed['process_sha256'] == candidate_digest
                      and candidate_digest != old_fixed,
                      'after-helper evidence did not prove different candidate bytes running')
            restored = h.updater_identity(old_fixed, 'false')
            h.require(snapshot() == baseline, 'different-bytes rollback changed application state')
            h.write(github, original_github.decode(), 0o755)
            h.write(helper, original_helper.decode(), 0o755)
            # The previous daemon intentionally supports ordinary Release Check,
            # not the candidate-only receipt introduced in this PR.
            check_id = owner_check_enqueue(h, password)
            h.wait_command(owner_env, check_id, {'succeeded'})
            checked = json.loads(h.sql(owner_env, "SELECT progress::text FROM platform_update_commands WHERE command_id='" + check_id + "';"))
            h.require(checked.get('immutable') is True and checked.get('source') != 'actions_candidate',
                      'old daemon did not recover ordinary Release Check')
            h.write(output / 'candidate-different-bytes-rollback.json', json.dumps(dict(
                passed=True, previous_sha256=old_fixed, candidate_sha256=candidate_digest,
                after_helper=observed, restored=restored, owner_check=check_id,
                application_unchanged=True, provenance='synthetic GitHub fixture'), indent=2))
            return
        # Busy refusal is a real queued command while daemon is stopped. Bootstrap
        # must refuse without helper invocation, then the normal daemon claims it.
        h.run(['systemctl', 'stop', h.PREFIX + 'updater.service'])
        busy_id = owner_check_enqueue(h, password)
        denied = h.run([binary, 'bootstrap-candidate'], env=environment, check=False)
        h.require(denied.returncode != 0 and 'no pending update' in denied.stderr,
                  'busy bootstrap did not fail closed')
        h.require(h.digest(h.LIB / 'updated') == old_fixed and snapshot() == baseline,
                  'busy refusal changed the application')
        h.run(['systemctl', 'start', h.PREFIX + 'updater.service'])
        h.wait_command(owner_env, busy_id, {'succeeded'})
        receipts['busy'] = dict(refused=True, recovered_check=busy_id)

        success = bootstrap()
        h.require(success.returncode == 0, 'candidate handoff failed: ' + h.sanitized(success.stderr))
        result = json.loads(success.stdout.strip().splitlines()[-1])
        h.require(result.get('pending_owner_check') is True and snapshot() == baseline,
                  'successful handoff changed application or skipped owner Check')
        receipts['success'] = dict(receipt=result, owner_check=owner_check())

        # A one-shot helper fixture delegates rollback to the unmodified helper.
        # No protected runtime timeout is lowered or disabled for these tests.
        for fault in ('helper_failure', 'enqueue_loss', 'handoff_timeout'):
            h.write(fixture / 'fault', fault)
            h.write(helper, '''#!/usr/bin/python3
import json, os, pathlib, subprocess, sys, time
r=pathlib.Path('/var/lib/ai-image-factory/updater/fixture'); marker=r/'fault'
if marker.exists():
 mode=marker.read_text(); marker.unlink()
 if mode=='helper_failure': raise SystemExit(42)
 if mode=='enqueue_loss':
  env=json.loads((r/'database.json').read_text())
  query="SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' AND granted AND classid=((hashtextextended('platform-system-update',0)>>32)&4294967295)::oid AND objid=(hashtextextended('platform-system-update',0)&4294967295)::oid AND objsubid=1;"
  subprocess.run(['psql','-X','-q','-v','ON_ERROR_STOP=1','-c',query],env=env,check=True,stdout=subprocess.DEVNULL)
 time.sleep(150)
os.execv(str(r/'native-helper'),[str(r/'native-helper'),*sys.argv[1:]])
''', 0o755)
            started = time.monotonic()
            failed = bootstrap()
            h.require(failed.returncode != 0, fault + ' incorrectly reported success')
            h.require(not (fixture / 'fault').exists(), fault + ' was never injected: '
                      + h.sanitized(failed.stderr)[-4096:])
            expected_error = {'helper_failure': '42', 'enqueue_loss': 'LeaseLost',
                              'handoff_timeout': 'candidate handoff timed out'}[fault]
            h.require(expected_error in failed.stderr, fault + ' failed for an unrelated reason: '
                      + h.sanitized(failed.stderr)[-4096:])
            h.require(h.digest(h.LIB / 'updated') == old_fixed and snapshot() == baseline,
                      fault + ' did not preserve old fixed/business/current/schema')
            h.updater_identity(old_fixed, 'false')
            receipts[fault] = dict(refused=True, elapsed_seconds=round(time.monotonic()-started, 3),
                                   owner_check=owner_check())
            h.write(helper, original_helper.decode(), 0o755)
        h.write(output / 'candidate-handoff.json', json.dumps(dict(
            passed=True, cases=receipts,
            boundaries=['Synthetic signed-tag and Actions provenance fixture; not cryptographic acceptance',
                        'Same updater bytes reinstalled; owner Check only, no second candidate Apply',
                        'No provider request']), indent=2))
    finally:
        h.write(github, original_github.decode(), 0o755)
        h.write(helper, original_helper.decode(), 0o755)
        updater.clear()
        updater.update(original_updater)
        h.env_file(h.CONFIG / 'updater.env', updater)
        if not different_bytes:
            h.run(['systemctl', 'restart', h.PREFIX + 'updater.service'])
