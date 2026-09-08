#!/usr/bin/env python3
"""Destructive *ephemeral CI only* release/recovery acceptance, using real services.

The only mocked trust boundary is GitHub release transport/attestation. The input
bundles, native updated/factoryctl, systemd, PostgreSQL 16, migrations, recovery,
independent reader and authenticated Next BFF are real. No provider task is sent.
Secure cookies are replayed explicitly by this HTTP client: this is authenticated
SSR/BFF acceptance, not a browser/TLS/SameSite acceptance claim.

Requires four prebuilt release inputs, the real pinned Codex runtime for the
baseline's startup dependency, root on a fresh GitHub-hosted Linux VM, and
AIF_NATIVE_TEST_ADMIN_DSN pointing at its synthetic loopback /postgres DB.
Never run this on a developer machine, persistent runner or production host.
"""

import argparse
import hashlib
from http.client import HTTPConnection, HTTPException
from http.cookies import SimpleCookie
import json
import os
from pathlib import Path
import pty
import pwd
import re
import select
import secrets
import shutil
import subprocess
import sys
import tarfile
import time
from urllib.parse import unquote, urlsplit
import uuid


REPO = Path(__file__).resolve().parents[1]
ROOT = Path('/opt/ai-image-factory')
STATE = Path('/var/lib/ai-image-factory')
CONFIG = Path('/etc/ai-image-factory')
LIB = Path('/usr/libexec/ai-image-factory')
UNITS = Path('/etc/systemd/system')
TARGET = 'x86_64-unknown-linux-gnu'
PREFIX = 'ai-image-factory-'
CODEX_VERSION = '0.153.4'
SECRET_VALUES = []
RECOVERY_HOST_FILES = {
    'ops/hooks/' + name for name in
    ('backup', 'recover', 'verify', 'verify-admin-reader', 'verify-gateway-runtime')
} | {
    'ops/systemd/' + PREFIX + name for name in
    ('updater.service', 'updater-recover@.service', 'recovery-gate.service')
}
MAX_UPDATER_EVENT_BYTES = 256 * 1024
MAX_UPDATER_EVENT_LINES = 800
MAX_DIAGNOSTIC_MESSAGE_CHARS = 4096
UPDATER_EVENT_FIELDS = ('command_id', 'action', 'target_version', 'phase', 'details',
                        'created_at_ms')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def progress(message):
    print(time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + ' ' + message, flush=True)


def sanitized(text):
    for value in sorted((value for value in SECRET_VALUES if value), key=len, reverse=True):
        text = text.replace(value, '[REDACTED]')
    return re.sub(r'postgres(?:ql)?://[^\s\"\']+', '[REDACTED-DSN]', text)


def run(arguments, *, env=None, stdin=None, timeout=120, check=True):
    result = subprocess.run([str(a) for a in arguments], input=stdin, text=True,
                            capture_output=True, env=env, timeout=timeout, check=False)
    if check and result.returncode:
        raise RuntimeError(f'{Path(str(arguments[0])).name} failed ({result.returncode}): '
                           + sanitized(result.stdout + result.stderr)[-12000:])
    return result


def write(path, text, mode=0o600):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')
    path.chmod(mode)


def bounded_updater_events(path):
    metadata = {'present': False, 'source_bytes': 0, 'truncated': False,
                'lines_read': 0, 'events_exported': 0, 'invalid_lines': 0}
    if not path.is_file() or path.is_symlink():
        return [], metadata
    size = path.stat().st_size
    offset = max(0, size - MAX_UPDATER_EVENT_BYTES)
    with path.open('rb') as handle:
        handle.seek(offset)
        data = handle.read(MAX_UPDATER_EVENT_BYTES)
    if offset:
        _, separator, data = data.partition(b'\n')
        if not separator:
            data = b''
    lines = data.splitlines()[-MAX_UPDATER_EVENT_LINES:]
    events = []
    invalid = 0
    for raw in lines:
        try:
            value = json.loads(sanitized(raw.decode('utf-8', errors='replace')))
        except (json.JSONDecodeError, UnicodeError):
            invalid += 1
            continue
        if not isinstance(value, dict):
            invalid += 1
            continue
        events.append({key: value[key] for key in UPDATER_EVENT_FIELDS if key in value})
    metadata.update(present=True, source_bytes=size,
                    truncated=offset > 0 or len(data.splitlines()) > MAX_UPDATER_EVENT_LINES,
                    lines_read=len(lines), events_exported=len(events), invalid_lines=invalid)
    return events, metadata


def limited_message(value):
    if value is None:
        return None
    return sanitized(str(value))[-MAX_DIAGNOSTIC_MESSAGE_CHARS:]


def failure_event_summary(events, command_id):
    selected = [event for event in events if event.get('command_id') == command_id]
    restoring_index = next((index for index, event in enumerate(selected)
                            if event.get('phase') == 'restoring'
                            and isinstance(event.get('details'), dict)), None)
    restore_required = next((event for event in reversed(selected)
                             if event.get('phase') == 'restore_required'
                             and isinstance(event.get('details'), dict)), None)
    original = {'phase': None, 'phase_basis': 'last_journal_phase_before_recovery',
                'message': None}
    if restoring_index is not None:
        restoring = selected[restoring_index]
        original['phase'] = (selected[restoring_index - 1].get('phase')
                             if restoring_index else None)
        original['message'] = limited_message(restoring['details'].get('error'))
    recovery = {'phase': None, 'message': None}
    if restore_required:
        recovery['phase'] = restore_required.get('phase')
        recovery['message'] = limited_message(
            restore_required['details'].get('recovery_error',
                                            restore_required['details'].get('error')))
    return {'original_apply': original, 'recover': recovery}


def fault_markers(fixture):
    verify_events = []
    verify_path = fixture / 'verify-runs.jsonl'
    if verify_path.is_file() and not verify_path.is_symlink():
        for raw in verify_path.read_text(encoding='utf-8', errors='replace').splitlines()[-100:]:
            try:
                value = json.loads(sanitized(raw))
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                verify_events.append({key: value.get(key) for key in
                                      ('release', 'scope', 'exit_code', 'reader_denial_42501')})
    candidate_validation_verify_seen = any(
        event.get('scope') == 'validation' for event in verify_events)
    post_validation = (fixture / 'fault-evidence.json').is_file()
    first_recovery = (fixture / 'recovery-fault-observed').is_file()
    return {'post_validation_fault_observed': post_validation,
            'first_recovery_fault_observed': first_recovery,
            'candidate_validation_verify_seen': candidate_validation_verify_seen,
            'verify_runs': verify_events}


def command_snapshot(environment, command_id):
    if not environment or not command_id:
        return None
    raw = sql(environment,
        "SELECT json_build_object('status',status,'phase',phase,'epoch',lease_epoch,"
        "'failure_code',failure_code,'failure_message',failure_message) "
        "FROM platform_update_commands WHERE command_id='"
        + str(uuid.UUID(command_id)) + "';")
    return json.loads(raw) if raw else None


def collect_recovery_evidence(output, diagnostics, original_error):
    command_id = diagnostics.get('failure_command')
    try:
        events, event_metadata = bounded_updater_events(STATE / 'updater/events.jsonl')
    except Exception as error:
        events = []
        event_metadata = {'present': False, 'source_bytes': 0, 'truncated': False,
                          'lines_read': 0, 'events_exported': 0, 'invalid_lines': 0,
                          'collection_error': limited_message(error)}
    observations = failure_event_summary(events, command_id)
    try:
        write(output / 'updater-events.jsonl', ''.join(
            json.dumps(event, separators=(',', ':'), ensure_ascii=False) + '\n'
            for event in events))
    except Exception as error:
        event_metadata['artifact_error'] = limited_message(error)
    command = None
    command_error = None
    try:
        command = command_snapshot(diagnostics.get('owner_environment'), command_id)
    except Exception as error:
        command_error = limited_message(error)
    if command:
        observations['recover']['command_phase'] = command.get('phase')
        observations['recover']['command_message'] = limited_message(command.get('failure_message'))
    try:
        markers = fault_markers(diagnostics.get('fixture', STATE / 'updater/fixture'))
        automatic_recovery_seen = any(
            event.get('command_id') == command_id
            and event.get('phase') in ('restoring', 'restore_required')
            for event in events)
        markers['automatic_recovery_seen_before_candidate_validation_verify'] = (
            automatic_recovery_seen and not markers['candidate_validation_verify_seen'])
        marker_error = None
    except Exception as error:
        markers = None
        marker_error = limited_message(error)
    evidence = {'failure_command': command_id, 'command': command,
                'command_snapshot_error': command_error,
                'updater_events': event_metadata, 'markers': markers,
                'marker_collection_error': marker_error, **observations}
    summary_path = output / 'summary.json'
    try:
        summary = json.loads(summary_path.read_text()) if summary_path.is_file() else {}
    except (OSError, json.JSONDecodeError):
        summary = {}
    summary.setdefault('passed', original_error is None)
    if original_error is not None:
        summary.setdefault('error', limited_message(original_error))
    summary['failure_evidence'] = evidence
    write(summary_path, sanitized(json.dumps(summary, indent=2, ensure_ascii=False)))


def collect_recovery_evidence_best_effort(output, diagnostics, original_error):
    try:
        collect_recovery_evidence(output, diagnostics, original_error)
        return None
    except Exception as error:
        message = limited_message(error)
        try:
            summary_path = output / 'summary.json'
            summary = json.loads(summary_path.read_text()) if summary_path.is_file() else {}
            summary.setdefault('passed', original_error is None)
            if original_error is not None:
                summary.setdefault('error', limited_message(original_error))
            summary['evidence_export_error'] = message
            write(summary_path, sanitized(json.dumps(summary, indent=2, ensure_ascii=False)))
        except Exception:
            pass
        return message


def digest(path):
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def pg_environment(dsn):
    parsed = urlsplit(dsn)
    require(parsed.scheme in ('postgres', 'postgresql')
            and parsed.hostname == '127.0.0.1' and parsed.username
            and parsed.password and parsed.path and not parsed.query
            and not parsed.fragment, 'only an explicit loopback synthetic PostgreSQL URL is allowed')
    return {'PATH': '/usr/sbin:/usr/bin:/sbin:/bin', 'LC_ALL': 'C',
            'PGHOST': '127.0.0.1', 'PGPORT': str(parsed.port or 5432),
            'PGUSER': unquote(parsed.username), 'PGPASSWORD': unquote(parsed.password),
            'PGDATABASE': unquote(parsed.path[1:]), 'PGCONNECT_TIMEOUT': '5'}


def sql(environment, statement, check=True):
    return run(['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1'], env=environment,
               stdin=statement, check=check).stdout.strip()


def preflight(args):
    require(sys.platform == 'linux' and os.geteuid() == 0, 'requires Linux root')
    require(os.environ.get('GITHUB_ACTIONS') == 'true'
            and os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted',
            'requires a disposable GitHub-hosted Actions runner')
    require(Path('/proc/1/comm').read_text().strip() == 'systemd',
            'PID 1 must be real systemd, not a container stub')
    runner_temp = Path(os.environ.get('RUNNER_TEMP', '')).resolve(strict=True)
    require(runner_temp.is_dir() and runner_temp != Path('/'), 'RUNNER_TEMP is required')
    output = args.output_dir.resolve()
    require(runner_temp in output.parents and not output.exists(),
            'output must be a new directory below RUNNER_TEMP')
    for path in (ROOT, STATE, CONFIG, LIB):
        require(not path.exists() and not path.is_symlink(), f'refusing existing install: {path}')
    require(not list(UNITS.glob(PREFIX + '*')), 'refusing existing Factory units')
    require(run(['getent', 'passwd', 'ai-image-factory'], check=False).returncode != 0,
            'refusing existing Factory service account')
    codex_runtime = args.codex_runtime.resolve(strict=True)
    require(runner_temp in codex_runtime.parents and codex_runtime.is_dir()
            and (codex_runtime / 'bin/codex').is_file(), 'Codex runtime must be the pinned runner-local input')
    require(run([codex_runtime / 'bin/codex', '--version'],
                env={'PATH': '/usr/bin:/bin'}, timeout=15).stdout.strip() == 'codex-cli ' + CODEX_VERSION,
            'the native startup dependency must be the real pinned Codex CLI')
    for command in ('psql', 'pg_dump', 'systemctl', 'ss', 'curl', 'node', 'openssl', 'runuser'):
        require(shutil.which(command), f'missing prerequisite {command}')
    require(Path('/usr/bin/node').is_file(), 'repo admin unit requires /usr/bin/node')
    admin_dsn = os.environ.get('AIF_NATIVE_TEST_ADMIN_DSN', '')
    environment = pg_environment(admin_dsn)
    require(environment['PGDATABASE'] == 'postgres', 'admin URL must select synthetic /postgres')
    require(sql(environment, 'SHOW server_version_num;').startswith('16'),
            'this acceptance job specifically requires PostgreSQL 16')
    require(sql(environment, "SELECT rolsuper FROM pg_roles WHERE rolname=current_user;") == 't',
            'synthetic PostgreSQL administrator is required')
    SECRET_VALUES.extend([admin_dsn, environment['PGPASSWORD']])
    output.mkdir(mode=0o700)
    return environment, output


def validate_bundle(bundle, manifest_path):
    manifest = json.loads(manifest_path.read_text())
    require(manifest['target_triple'] == TARGET, 'requires native x86_64 Linux bundles')
    version = manifest['release_version']
    require(re.fullmatch(r'[A-Za-z0-9._-]{1,100}', version), 'invalid release version')
    require(digest(bundle) == manifest['bundle_sha256']
            and bundle.stat().st_size == manifest['bundle_bytes'], 'input bundle digest mismatch')
    with tarfile.open(bundle) as archive:
        members = archive.getmembers()
        for member in members:
            name = Path(member.name)
            require(not name.is_absolute() and '..' not in name.parts
                    and (member.isfile() or member.isdir()), 'unsafe input archive')
        file_members = [m for m in members if m.isfile()]
        regular = {m.name.removeprefix('./'): m for m in file_members}
        require(len(regular) == len(file_members) == len(manifest['files']),
                'duplicate archive or manifest file entries')
        require(set(regular) == {f['path'] for f in manifest['files']},
                'input archive file set differs from manifest')
        for item in manifest['files']:
            member = regular[item['path']]
            require(member.size == item['bytes'] and member.mode == item['mode']
                    and member.mode in (0o644, 0o755),
                    'input release file size/mode mismatch')
            with archive.extractfile(member) as handle:
                require(hashlib.file_digest(handle, 'sha256').hexdigest() == item['sha256'],
                        'input release file digest mismatch')
    return manifest


def unpack(bundle, destination):
    destination.mkdir(parents=True)
    run(['tar', '-xzf', bundle, '-C', destination, '--no-same-owner'])


def env_file(path, values):
    require(all('\n' not in str(v) and '"' not in str(v) and '\\' not in str(v)
                for v in values.values()), 'unsupported environment file value')
    write(path, ''.join(f'{k}="{v}"\n' for k, v in values.items()))


def bootstrap(binary, environment, password):
    child, master = pty.fork()
    if child == 0:
        os.execve(str(binary), [str(binary), 'bootstrap-admin', 'owner@native.invalid',
                               'Native CI Owner'], environment)
    buffer = b''
    sent = 0
    deadline = time.monotonic() + 60
    try:
        while time.monotonic() < deadline:
            ready, _, _ = select.select([master], [], [], 0.25)
            if ready:
                try:
                    chunk = os.read(master, 4096)
                except OSError:
                    chunk = b''
                buffer += chunk
                prompts = [b'New administrator password:', b'Confirm administrator password:']
                if sent < 2 and prompts[sent] in buffer:
                    os.write(master, password.encode() + b'\n')
                    sent += 1
                    buffer = b''
            waited, status = os.waitpid(child, os.WNOHANG)
            if waited:
                require(sent == 2 and os.waitstatus_to_exitcode(status) == 0,
                        'real TTY owner bootstrap failed: ' + sanitized(buffer.decode(errors='replace')))
                return
        os.kill(child, 9)
        os.waitpid(child, 0)
        raise RuntimeError('owner bootstrap timeout')
    finally:
        os.close(master)


def http(port, path, *, method='GET', body=None, headers=None):
    request_headers = dict(headers or {})
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        request_headers['Content-Type'] = 'application/json'
    connection = HTTPConnection('127.0.0.1', port, timeout=15)
    try:
        connection.request(method, path, body=data, headers=request_headers)
        response = connection.getresponse()
        return response.status, response.read(), response.getheaders()
    finally:
        connection.close()


def wait_http(port, path, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if http(port, path)[0] == 200:
                return
        except (OSError, HTTPException):
            pass
        time.sleep(1)
    raise RuntimeError(f'HTTP readiness timeout on loopback port {port}')


def authenticated_acceptance(password, account_id):
    require(http(8787, '/admin/v1/provider-accounts')[0] == 401
            and http(3010, '/api/gateway/admin/v1/provider-accounts')[0] == 401,
            'admin data unexpectedly accessible without authentication')
    status, data, _ = http(8787, '/admin/v1/auth/login', method='POST', body={
        'email': 'owner@native.invalid', 'password': password,
        'client_id': 'ai-image-factory-admin-bff'})
    require(status == 200, 'gateway owner login failed')
    token = json.loads(data)['access_token']
    SECRET_VALUES.append(token)
    evidence = {'api': {}, 'bff': {}, 'authenticated_page_shells': {}}
    for path in ('overview', 'provider-accounts', 'usage', 'system/update'):
        status, data, _ = http(8787, '/admin/v1/' + path,
                               headers={'Authorization': 'Bearer ' + token})
        require(status == 200 and isinstance(json.loads(data), (dict, list)),
                'authenticated admin read failed: ' + path)
        require(b'Admin read store is unavailable' not in data, 'admin store unavailable')
        evidence['api'][path] = {'status': status, 'bytes': len(data)}
        if path == 'provider-accounts':
            require(any(a.get('provider_account_id') == account_id and a.get('account_key') == 'ci-account'
                        for a in json.loads(data).get('accounts', [])),
                    'gateway did not return the persisted synthetic provider account')
            evidence['api'][path]['persisted_account_found'] = True
    # Explicit Cookie replay tests the real production BFF without changing its
    # Secure/SameSite settings. Browser TLS policy is deliberately not claimed.
    cookies = SimpleCookie()
    status, _, response_headers = http(3010, '/api/session')
    require(status == 200, 'BFF session initialization failed')
    for key, value in response_headers:
        if key.lower() == 'set-cookie':
            cookies.load(value)
    csrf = next((m.value for name, m in cookies.items() if name.endswith('aif_csrf')), None)
    require(csrf, 'real BFF did not issue CSRF cookie')
    headers = {'Origin': 'http://127.0.0.1:3010', 'Sec-Fetch-Site': 'same-origin',
               'x-aif-csrf': csrf,
               'Cookie': '; '.join(f'{k}={v.value}' for k, v in cookies.items())}
    status, data, response_headers = http(3010, '/api/session', method='POST', headers=headers,
                                          body={'email': 'owner@native.invalid', 'password': password})
    require(status == 200 and json.loads(data).get('authenticated'), 'real BFF owner login failed')
    for key, value in response_headers:
        if key.lower() == 'set-cookie':
            cookies.load(value)
            SECRET_VALUES.extend(m.value for m in cookies.values() if m.value)
    cookie_header = {'Cookie': '; '.join(f'{k}={v.value}' for k, v in cookies.items())}
    for path in ('overview', 'provider-accounts', 'usage'):
        status, data, _ = http(3010, '/api/gateway/admin/v1/' + path, headers=cookie_header)
        require(status == 200 and isinstance(json.loads(data), (dict, list)),
                'authenticated BFF read failed: ' + path)
        evidence['bff'][path] = {'status': status, 'bytes': len(data)}
        if path == 'provider-accounts':
            require(any(a.get('provider_account_id') == account_id and a.get('account_key') == 'ci-account'
                        for a in json.loads(data).get('accounts', [])),
                    'BFF did not return the persisted synthetic provider account')
            evidence['bff'][path]['persisted_account_found'] = True
    for path in ('overview', 'provider-accounts', 'billing'):
        status, data, _ = http(3010, '/' + path, headers=cookie_header)
        require(status == 200 and b'<html' in data and b'Admin read store is unavailable' not in data,
                'authenticated admin page failed: ' + path)
        evidence['authenticated_page_shells'][path] = {'status': status, 'bytes': len(data)}
    return token, evidence


def security_snapshot(environment):
    # Independent catalog comparison; no OID or row ordering dependence. Native
    # backup additionally checks its more extensive object contract in-transaction.
    statement = """
SET search_path=pg_catalog;
WITH objects(kind,name,owner,acl) AS (
 SELECT 'schema',nspname,nspowner,nspacl FROM pg_namespace WHERE nspname='public'
 UNION ALL SELECT 'relation:'||relkind::text,relname,relowner,relacl FROM pg_class WHERE relnamespace='public'::regnamespace
 UNION ALL SELECT 'column',c.relname||'.'||a.attname,c.relowner,a.attacl FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relnamespace='public'::regnamespace AND a.attnum>0 AND NOT a.attisdropped
 UNION ALL SELECT 'function',oid::regprocedure::text,proowner,proacl FROM pg_proc WHERE pronamespace='public'::regnamespace
 UNION ALL SELECT 'type',typname,typowner,typacl FROM pg_type WHERE typnamespace='public'::regnamespace
 UNION ALL SELECT 'default:'||defaclobjtype::text,defaclrole::regrole::text,defaclrole,defaclacl FROM pg_default_acl WHERE defaclnamespace='public'::regnamespace
), items AS (
 SELECT jsonb_build_array(kind,name,owner::regrole::text,
   (SELECT jsonb_agg(item::text ORDER BY item::text) FROM unnest(acl) item)) value FROM objects
 UNION ALL SELECT jsonb_build_array('extension',extname,extowner::regrole::text,nspname,extversion) FROM pg_extension JOIN pg_namespace ON pg_namespace.oid=extnamespace WHERE extname='btree_gist'
) SELECT jsonb_agg(value ORDER BY value::text) FROM items;
"""
    return json.loads(sql(environment, statement))


def artifact_snapshot():
    return {str(p.relative_to(STATE / 'artifacts')): {'sha256': digest(p), 'bytes': p.stat().st_size}
            for p in sorted((STATE / 'artifacts').rglob('*')) if p.is_file()}


def install_fixtures(candidate, candidate_bundle, candidate_manifest, owner_environment):
    fixtures = STATE / 'updater/fixture'
    fixtures.mkdir(mode=0o700)
    for source in (candidate_bundle, candidate_manifest):
        shutil.copy2(source, fixtures / source.name)
    write(fixtures / 'candidate.json', json.dumps(candidate))
    write(fixtures / 'database.json', json.dumps(owner_environment))
    # This is the *only* trust/transport fake. All accepted calls are auditable;
    # unrecognized invocation fails rather than silently succeeding.
    write(LIB / 'ci-gh', '''#!/usr/bin/python3
import json, pathlib, shutil, sys
r=pathlib.Path('/var/lib/ai-image-factory/updater/fixture')
m=json.loads((r/'candidate.json').read_text()); a=sys.argv[1:]
with (r/'gh-boundary.jsonl').open('a') as f: f.write(json.dumps(a)+'\\n')
if len(a)==2 and a[0]=='api' and a[1] in ('repos/fixture/native/releases/latest','repos/fixture/native/releases/tags/'+m['release_version']):
 print(json.dumps(dict(tag_name=m['release_version'],draft=False,prerelease=False,immutable=True)))
elif a[:2]==['release','download'] and a[2]==m['release_version']:
 dest=pathlib.Path(a[a.index('--dir')+1]); assert str(dest).startswith('/opt/ai-image-factory/staging/')
 names=[a[i+1] for i,v in enumerate(a) if v=='--pattern']
 expected=['ai-image-factory-'+m['release_version']+'-'+m['target_triple']+s for s in ('.manifest.json','.tar.gz')]
 assert sorted(names)==sorted(expected)
 for name in names: shutil.copyfile(r/name,dest/name)
elif a[:2] in (['release','verify'],['release','verify-asset'],['attestation','verify']):
 # Explicit fixture boundary: no cryptographic attestation claim is made.
 assert '--repo' in a and a[a.index('--repo')+1]=='fixture/native'
 if a[:2]==['attestation','verify']:
  assert '--deny-self-hosted-runners' in a
  assert a[a.index('--source-digest')+1]==m['commit_sha']
  assert a[a.index('--source-ref')+1]=='refs/tags/'+m['release_version']
else: raise SystemExit('unrecognized GitHub fixture invocation')
''', 0o755)
    write(LIB / 'ci-verify', '''#!/usr/bin/python3
import json, os, pathlib, subprocess, sys
r=pathlib.Path('/var/lib/ai-image-factory/updater/fixture')
result=subprocess.run(['/usr/libexec/ai-image-factory/hooks/verify'],capture_output=True,text=True)
sys.stdout.write(result.stdout); sys.stderr.write(result.stderr)
with (r/'verify-runs.jsonl').open('a') as f:
 f.write(json.dumps(dict(release=pathlib.Path('/opt/ai-image-factory/current').resolve().name,scope=os.environ.get('AIF_UPDATE_PROCESS_SCOPE'),exit_code=result.returncode,reader_denial_42501='database write denial 42501' in result.stdout))+'\\n')
if result.returncode: raise SystemExit(result.returncode)
m=json.loads((r/'candidate.json').read_text())
current=pathlib.Path('/opt/ai-image-factory/current').resolve().name
if (r/'inject-validation-failure').exists() and current==m['release_version'] and os.environ.get('AIF_UPDATE_PROCESS_SCOPE')=='validation':
 (r/'inject-validation-failure').unlink()
 db=json.loads((r/'database.json').read_text())
 query="UPDATE public.ci_recovery_probe SET payload='candidate-corruption'; CREATE TABLE public.ci_candidate_only(id integer); REVOKE USAGE ON SCHEMA public FROM " + (r/'reader-role').read_text().strip() + ";"
 subprocess.run(['psql','-X','-q','-v','ON_ERROR_STOP=1'],input=query,text=True,env=db,check=True)
 pathlib.Path('/var/lib/ai-image-factory/artifacts/ci-original.bin').write_bytes(b'candidate-corruption')
 pathlib.Path('/var/lib/ai-image-factory/artifacts/ci-candidate-only.bin').write_bytes(b'candidate-only')
 (r/'fail-first-recover').write_text('injected before native recover\\n')
 (r/'fault-evidence.json').write_text(json.dumps(dict(current=current,scope='validation',real_verify_passed=True,exit_code=42)))
 raise SystemExit(42)
''', 0o755)
    write(LIB / 'ci-recover', '''#!/usr/bin/python3
import os, pathlib
r=pathlib.Path('/var/lib/ai-image-factory/updater/fixture')
if (r/'fail-first-recover').exists():
 (r/'fail-first-recover').unlink()
 (r/'recovery-fault-observed').write_text('43\\n')
 raise SystemExit(43)
os.execv('/usr/libexec/ai-image-factory/hooks/recover',['recover'])
''', 0o755)
    for action, value in (('close', 'closed'), ('open', 'open')):
        write(LIB / ('ci-admission-' + action), '#!/bin/sh\nset -eu\n'
              f"printf '{value}\\n' > /var/lib/ai-image-factory/updater/admission\n", 0o755)
    write(LIB / 'ci-admission-proxy', '''#!/usr/bin/python3
import http.client, http.server, pathlib
class Handler(http.server.BaseHTTPRequestHandler):
 def do_GET(self):
  if pathlib.Path('/var/lib/ai-image-factory/updater/admission').read_text().strip()!='open':
   self.send_response(503); self.end_headers(); self.wfile.write(b'admission closed'); return
  upstream=http.client.HTTPConnection('127.0.0.1',8787,timeout=5)
  try:
   upstream.request('GET',self.path); response=upstream.getresponse(); data=response.read()
   self.send_response(response.status); self.end_headers(); self.wfile.write(data)
  finally: upstream.close()
 def log_message(self,*args): pass
http.server.ThreadingHTTPServer(('127.0.0.1',8788),Handler).serve_forever()
''', 0o755)
    write(UNITS / 'aif-native-admission.service', '[Unit]\nDescription=Isolated CI admission endpoint\n'
          '[Service]\nExecStart=/usr/libexec/ai-image-factory/ci-admission-proxy\n', 0o644)
    write(STATE / 'updater/admission', 'open\n')


def unit_evidence(recovery_command_id='00000000-0000-0000-0000-000000000001'):
    evidence = {}
    for name in ('updater.service', 'updater-recover@' + recovery_command_id + '.service', 'recovery-gate.service'):
        unit = PREFIX + name
        output = run(['systemctl', 'show', unit, '-p', 'ReadWritePaths', '-p', 'ProtectSystem',
                      '-p', 'NoNewPrivileges', '-p', 'PrivateTmp', '-p', 'DropInPaths',
                      '-p', 'BindPaths', '-p', 'BindReadOnlyPaths',
                      '-p', 'FragmentPath', '-p', 'ExecStart']).stdout
        fields = dict(line.split('=', 1) for line in output.splitlines() if '=' in line)
        require(set(fields['ReadWritePaths'].split()) == {str(ROOT), str(STATE)},
                'effective recovery sandbox differs from exact parent-only contract')
        require(fields['ProtectSystem'] == 'strict' and fields['NoNewPrivileges'] == 'yes'
                and fields['PrivateTmp'] == 'yes' and fields['DropInPaths'] == '',
                'effective recovery hardening/drop-ins differ from repository units')
        require(fields['BindPaths'] == '' and fields['BindReadOnlyPaths'] == '',
                'effective recovery unit contains an unexpected explicit bind mount')
        evidence[unit] = fields
    return evidence


def install_host_files(bundle, manifest, selected=None):
    installed = {}
    expected = {item['path']: item for item in manifest['files']}
    with tarfile.open(bundle) as archive:
        for member in archive.getmembers():
            name = member.name.removeprefix('./')
            if not member.isfile() or (selected is not None and name not in selected):
                continue
            if name.startswith('ops/hooks/') and name.count('/') == 2:
                destination = LIB / 'hooks' / Path(name).name
            elif name.startswith('ops/systemd/') and name.count('/') == 2 and Path(name).suffix in ('.service', '.target'):
                destination = UNITS / Path(name).name
            elif name == 'bin/updated':
                destination = LIB / 'updated'
            elif name == 'ops/upgrade-updater':
                destination = LIB / 'upgrade-updater'
            else:
                continue
            with archive.extractfile(member) as source, destination.open('wb') as target:
                shutil.copyfileobj(source, target)
            destination.chmod(expected[name]['mode'])
            actual = digest(destination)
            require(actual == expected[name]['sha256'], 'installed host file differs from release manifest')
            installed[name] = {'destination': str(destination), 'sha256': actual}
    if selected is not None:
        require(set(installed) == selected, 'candidate is missing required recovery preparation files')
    else:
        require('bin/updated' in installed and 'ops/upgrade-updater' in installed,
                'baseline is missing the fixed updater or its native upgrade helper')
    return installed


def updater_identity(expected_hash, apply_enabled, *, require_safe_mounts=True):
    started = time.monotonic()
    deadline = started + 10
    samples = []

    def service_state():
        result = run(['systemctl', 'show', PREFIX + 'updater.service', '-p', 'MainPID',
                      '-p', 'ActiveState', '-p', 'NRestarts', '-p', 'InvocationID'],
                     timeout=max(0.001, min(2, deadline - time.monotonic())))
        return dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)

    initial = service_state()
    pid = initial['MainPID']
    require(re.fullmatch('[1-9][0-9]*', pid), 'updater has no real MainPID')
    process = Path('/proc') / pid
    state = initial
    while True:
        require(state == initial and state['ActiveState'] == 'active',
                'updater exited or restarted during identity readiness: '
                + json.dumps({'expected_sha256': expected_hash, 'state': state, 'samples': samples}))
        try:
            executable = (process / 'exe').resolve(strict=True)
            actual_hash = digest(process / 'exe')
        except OSError as error:
            raise RuntimeError('updater process disappeared during identity readiness: '
                               + json.dumps({'state': state, 'samples': samples})) from error
        sample = {'expected_executable': str(LIB / 'updated'), 'expected_sha256': expected_hash,
                  'actual_executable': str(executable), 'actual_sha256': actual_hash, **state}
        if not samples or sample != samples[-1]:
            samples.append(sample)
            progress('Updater identity sample: ' + json.dumps(sample))
        detail = json.dumps(sample)
        if executable == LIB / 'updated':
            require(actual_hash == expected_hash, 'running updater SHA mismatch: ' + detail)
            break
        require(time.monotonic() < deadline, 'updater exec readiness timeout: ' + detail)
        # Type=simple reports startup before execve; this same PID may still be
        # systemd-executor. Never accept a different path or a replacement PID.
        time.sleep(max(0, min(0.1, deadline - time.monotonic())))
        require(time.monotonic() < deadline, 'updater exec readiness timeout: ' + detail)
        state = service_state()
    environment = dict(entry.split(b'=', 1) for entry in (process / 'environ').read_bytes().split(b'\0') if b'=' in entry)
    require(environment.get(b'AIF_UPDATE_APPLY_ENABLED') == apply_enabled.encode(),
            'running updater Apply policy differs from the expected phase')
    state_mountpoints = []
    for line in (process / 'mountinfo').read_text().splitlines():
        fields = line.split()
        require(len(fields) >= 6, 'malformed updater mountinfo')
        mountpoint = re.sub(r'\\([0-7]{3})', lambda match: chr(int(match[1], 8)), fields[4])
        if mountpoint == str(STATE) or mountpoint.startswith(str(STATE) + '/'):
            state_mountpoints.append(mountpoint)
    if require_safe_mounts:
        artifact = str(STATE / 'artifacts')
        require(not any(path == artifact or path.startswith(artifact + '/') for path in state_mountpoints),
                'actual updater namespace contains an artifact mountpoint that prevents safe recovery')
    final = service_state()
    require(final == initial and final['ActiveState'] == 'active'
            and (process / 'exe').resolve(strict=True) == executable
            and digest(process / 'exe') == expected_hash,
            'updater identity changed while reading policy/mounts: ' + json.dumps(final))
    return {'pid': int(pid), 'executable': str(executable), 'sha256': expected_hash,
            'apply_enabled': apply_enabled, 'state_mountpoints': sorted(state_mountpoints),
            'startup_samples': samples, 'identity_wait_seconds': round(time.monotonic() - started, 3)}


def prepare_recovery_host_files(candidate_bundle, candidate, environment, installed):
    # Mirror the separately authorized runbook maintenance window exactly. A
    # fresh CI baseline has no old-format recovery to finish; production must
    # prove the same condition rather than converting an incomplete old backup.
    require('AIF_UPDATE_APPLY_ENABLED="false"' in (CONFIG / 'update-policy.env').read_text(),
            'host preparation requires Apply=false')
    run(['systemctl', 'stop', PREFIX + 'updater.service'])
    require(run(['systemctl', 'show', PREFIX + 'updater.service', '-p', 'MainPID', '--value']).stdout.strip() == '0',
            'updater is still running before fixed file replacement')
    require(sql(environment, "SELECT count(*) FROM platform_update_commands WHERE status IN ('queued','running','restoring','restore_required');") == '0',
            'pending update must be resolved before changing recovery hooks')
    descriptors = STATE / 'updater/recovery'
    require(not descriptors.exists() or (descriptors.is_dir() and not descriptors.is_symlink()
            and not list(descriptors.iterdir())), 'protected recovery descriptor must be resolved first')
    saved = STATE / 'updater/host-preparation-backup'
    saved.mkdir(mode=0o700)
    previous = {}
    for name in sorted(RECOVERY_HOST_FILES):
        if name in installed:
            source = Path(installed[name]['destination'])
            require(digest(source) == installed[name]['sha256'], 'baseline fixed file drift before preparation')
            destination = saved / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            previous[name] = {'existed': True, 'sha256': digest(destination),
                              'backup': str(destination), 'mode': source.stat().st_mode & 0o777}
        else:
            require(name == 'ops/hooks/verify-admin-reader' and not (LIB / 'hooks/verify-admin-reader').exists(),
                    'unexpected missing baseline recovery file')
            previous[name] = {'existed': False}
    write(saved / 'manifest.json', json.dumps(previous, indent=2))
    prepared = install_host_files(candidate_bundle, candidate, RECOVERY_HOST_FILES)
    for name in ('bin/updated', 'ops/hooks/quiesce', 'ops/hooks/start-processes'):
        require(digest(Path(installed[name]['destination'])) == installed[name]['sha256'],
                'recovery preparation changed a file outside its five-hook/three-unit scope')
    run(['systemctl', 'daemon-reload'])
    return prepared, {'apply_disabled': True, 'updater_stopped': True,
                      'pending_commands': 0, 'protected_descriptors': 0, 'previous': previous}


def updater_hook_environment():
    pid = run(['systemctl', 'show', PREFIX + 'updater.service', '-p', 'MainPID', '--value']).stdout.strip()
    require(re.fullmatch('[1-9][0-9]*', pid), 'real updater PID missing')
    values = {}
    for entry in (Path('/proc') / pid / 'environ').read_bytes().split(b'\0'):
        key, separator, value = entry.partition(b'=')
        if separator and key.startswith(b'AIF_UPDATE_') and key.endswith(b'_HOOK'):
            values[key.decode()] = value.decode()
    for name in ('QUIESCE', 'RESUME', 'BACKUP', 'ACTIVATE'):
        require(values.get('AIF_UPDATE_' + name + '_HOOK') == str(LIB / 'hooks' / name.lower()),
                'running updater is not wired to the candidate-installed fixed hook')
    require(values.get('AIF_UPDATE_VERIFY_HOOK') == str(LIB / 'ci-verify')
            and values.get('AIF_UPDATE_RECOVER_HOOK') == str(LIB / 'ci-recover'),
            'explicit verify/recover fault wrappers not installed')
    return values


def wait_command(environment, command_id, expected, timeout=480):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = command_snapshot(environment, command_id)
        if last:
            if last['status'] in expected:
                return last
            require(last['status'] not in ('failed', 'restore_required', 'restored', 'succeeded'),
                    'unexpected command terminal state: ' + json.dumps(last))
        time.sleep(1)
    raise RuntimeError('updater command timeout: ' + json.dumps(last))


def enqueue(token, action, version=None):
    status, data, _ = http(8787, '/admin/v1/system/update/' + action, method='POST',
        body={'target_version': version} if version else None,
        headers={'Authorization': 'Bearer ' + token, 'Idempotency-Key': 'native-' + uuid.uuid4().hex})
    require(status == 200, 'authenticated update enqueue failed: ' + action)
    return json.loads(data)['command_id']


def execute(args, admin, output, diagnostics):
    started = time.monotonic()
    progress('Validating release bundle contents; no GitHub signature claim is made by this fixture job')
    baseline = validate_bundle(args.baseline_bundle, args.baseline_manifest)
    candidate = validate_bundle(args.candidate_bundle, args.candidate_manifest)
    require(baseline['target_schema_version'] < candidate['target_schema_version'],
            'requires a genuine old-schema baseline, not renamed same-build fixtures')
    require(baseline['commit_sha'] != candidate['commit_sha'], 'baseline/candidate commits must differ')
    progress(f"Installing original baseline host/runtime and real schema {baseline['target_schema_version']}")
    for directory in (ROOT / 'releases', STATE, CONFIG, LIB / 'hooks'):
        directory.mkdir(parents=True, mode=0o755)
    # pathlib creates missing parents with 0777 subject to the runner umask.
    # Mirror install-release before the old updater trusts candidate binaries.
    for protected in (ROOT.parent, ROOT):
        protected.chmod(0o755)
    for protected in (Path('/'), ROOT.parent, ROOT):
        metadata = protected.stat()
        require(metadata.st_uid == 0 and metadata.st_mode & 0o022 == 0,
                f'synthetic install ancestor is not protected: {protected}')
    run(['useradd', '--system', '--home-dir', str(STATE), '--shell', '/usr/sbin/nologin',
         'ai-image-factory'])
    service = pwd.getpwnam('ai-image-factory')
    for directory in ('artifacts', 'admin-runtime', 'runner', 'credentials/grok', 'provider-homes'):
        path = STATE / directory
        path.mkdir(parents=True, mode=0o700)
        os.chown(path, service.pw_uid, service.pw_gid)
    (STATE / 'credentials').chmod(0o755)
    (STATE / 'updater').mkdir(mode=0o700)
    (STATE / 'backups').mkdir(mode=0o700)
    unpack(args.baseline_bundle, ROOT / 'releases' / baseline['release_version'])
    # ProtectHome hides /home/runner from the real service. Keep the complete
    # pinned runtime in this disposable installation, outside immutable releases.
    codex_runtime = ROOT / 'provider-tools/codex' / CODEX_VERSION
    shutil.copytree(args.codex_runtime, codex_runtime)
    codex_binary = codex_runtime / 'bin/codex'
    codex_dependency = {'version': CODEX_VERSION, 'executable': str(codex_binary),
                        'sha256': digest(codex_binary), 'purpose': 'startup prerequisite only'}
    require(codex_dependency['sha256'] == digest(args.codex_runtime / 'bin/codex'),
            'copied Codex startup dependency differs from pinned input')
    service_version = run(['runuser', '-u', service.pw_name, '--', '/usr/bin/env', '-i',
                           'PATH=/usr/bin:/bin', codex_binary, '--version'],
                          env={'PATH': '/usr/sbin:/usr/bin:/sbin:/bin'}, timeout=15)
    require(service_version.stdout.strip() == 'codex-cli ' + CODEX_VERSION,
            'service UID cannot execute the copied pinned Codex startup dependency')
    codex_dependency['service_uid_version_check'] = {
        'uid': service.pw_uid, 'user': service.pw_name,
        'version_output': service_version.stdout.strip(), 'exit_code': service_version.returncode}
    progress('Real pinned startup dependency: ' + json.dumps(codex_dependency))
    installed_host_files = install_host_files(args.baseline_bundle, baseline)
    baseline_host_files = dict(installed_host_files)
    baseline_updater_hash = installed_host_files['bin/updated']['sha256']
    candidate_updater_hash = next(item['sha256'] for item in candidate['files'] if item['path'] == 'bin/updated')
    (ROOT / 'current').symlink_to(ROOT / 'releases' / baseline['release_version'])
    suffix = uuid.uuid4().hex[:12]
    database, owner, reader, second = (f'aif_native_{suffix}{ending}' for ending in ('', '_owner', '_reader', '_other'))
    password, db_password = 'Ci9!' + secrets.token_hex(20), secrets.token_hex(24)
    SECRET_VALUES.extend([password, db_password])
    sql(admin, f"CREATE ROLE {owner} LOGIN PASSWORD '{db_password}'; CREATE ROLE {reader} LOGIN NOINHERIT PASSWORD '{db_password}'; CREATE ROLE {second}; GRANT {second} TO {owner};")
    sql(admin, f'CREATE DATABASE {database} OWNER {owner};')
    host_port = '127.0.0.1:' + admin['PGPORT']
    dsn = f'postgresql://{owner}:{db_password}@{host_port}/{database}'
    reader_dsn = f'postgresql://{reader}:{db_password}@{host_port}/{database}'
    SECRET_VALUES.extend([dsn, reader_dsn])
    owner_env = pg_environment(dsn)
    diagnostics['owner_environment'] = owner_env
    sql(owner_env, f'ALTER SCHEMA public OWNER TO {owner}; REVOKE CREATE ON SCHEMA public FROM PUBLIC;')
    application = {'PATH': str(codex_runtime / 'bin') + ':/usr/sbin:/usr/bin:/sbin:/bin', 'DATABASE_URL': dsn,
        'GATEWAY_DATABASE_SCHEMA': 'public', 'GATEWAY_ARTIFACT_ROOT': str(STATE / 'artifacts'),
        'GATEWAY_PROVIDER_HOME_ROOT': str(STATE / 'provider-homes'),
        'GATEWAY_MANAGED_CODEX_EXECUTABLE': str(codex_binary),
        'GATEWAY_ADMIN_READ_DATABASE_URL': reader_dsn, 'GATEWAY_BIND': '127.0.0.1:8787',
        'GATEWAY_API_TOKEN': secrets.token_hex(32), 'GATEWAY_API_KEY_PEPPERS': '1:' + secrets.token_hex(32),
        'GATEWAY_API_KEY_CURRENT_PEPPER_VERSION': '1', 'GATEWAY_IDENTITY_ENABLED': 'true',
        'GATEWAY_IMAGES_GENERATION_CONTRACT': 'customer-pricing-v4',
        'GATEWAY_WEBHOOK_SIGNING_KEYS': '1:' + secrets.token_hex(32),
        'GATEWAY_WEBHOOK_CURRENT_SIGNING_KEY_VERSION': '1',
        'GATEWAY_AUTH_ISSUER': 'http://127.0.0.1:8787', 'GATEWAY_AUTH_AUDIENCE': 'ai-image-factory-admin',
        'GATEWAY_AUTH_CLIENT_ID': 'ai-image-factory-admin-bff', 'GATEWAY_JWT_ACTIVE_KID': 'admin-es256-v1',
        'GATEWAY_JWT_PRIVATE_KEY_PATH': str(STATE / 'identity/admin-jwt-es256-private.pem'),
        'GATEWAY_JWT_PUBLIC_KEYS': 'admin-es256-v1:' + str(STATE / 'identity/admin-jwt-es256-public.pem'),
        'GATEWAY_REFRESH_TOKEN_CURRENT_PEPPER_VERSION': '1',
        'GATEWAY_REFRESH_TOKEN_PEPPERS_PATH': str(STATE / 'identity/refresh-token-peppers'),
        'GATEWAY_CODEX_QUOTA_AUTO_REFRESH_ENABLED': 'false', 'GATEWAY_BBOX_ENABLED': 'false',
        'RECONCILER_INTERVAL_MS': '1000', 'RUST_LOG': 'info'}
    SECRET_VALUES.extend([application['GATEWAY_API_TOKEN'], application['GATEWAY_API_KEY_PEPPERS']])
    SECRET_VALUES.extend([application['GATEWAY_WEBHOOK_SIGNING_KEYS'],
                          application['GATEWAY_WEBHOOK_SIGNING_KEYS'].split(':', 1)[1]])
    run([ROOT / 'current/bin/factoryctl', 'migrate'], env=application, timeout=180)
    require(int(sql(owner_env, 'SELECT max(version) FROM _sqlx_migrations;')) == baseline['target_schema_version'],
            'real baseline migration ledger differs from release manifest')
    run([REPO / 'scripts/generate-admin-identity-secrets.sh', STATE / 'identity', 'admin-es256-v1'])
    for path in [STATE / 'identity', *(STATE / 'identity').rglob('*')]:
        os.chown(path, service.pw_uid, service.pw_gid)
    bootstrap(ROOT / 'current/bin/factoryctl', application, password)
    auth_path = STATE / 'credentials/grok/auth.json'
    write(auth_path, '{"access_token":"synthetic-never-send","refresh_token":"synthetic-never-send"}\n')
    # The unit uses %I (unescaped instance); a literal hyphen would become '/'.
    executor = {'EXECUTOR_PROFILE_KEY': 'ci.grok', 'EXECUTOR_CREDENTIAL_POOL_KEY': 'ci-pool',
        'EXECUTOR_PROVIDER_ACCOUNT_KEY': 'ci-account', 'EXECUTOR_CREDENTIAL_REF': 'ci.synthetic.grok',
        'EXECUTOR_CREDENTIAL_REVISION': '1', 'EXECUTOR_MAX_CONCURRENCY': '1',
        'EXECUTOR_GROK_CREDENTIAL_HOME': str(auth_path.parent),
        'EXECUTOR_GROK_EXECUTABLE': str(ROOT / 'current/bin/grok'),
        'EXECUTOR_HELPER_EXECUTABLE': str(ROOT / 'current/bin/grok-runner'),
        'EXECUTOR_RUNNER_ROOT': str(STATE / 'runner')}
    run([ROOT / 'current/bin/factoryctl', 'provision-grok-profile'], env=application | executor)
    os.chown(auth_path, service.pw_uid, service.pw_gid)
    account_id = sql(owner_env, "SELECT provider_account_id FROM provider_accounts WHERE account_key='ci-account';")
    require(str(uuid.UUID(account_id)) == account_id, 'synthetic provider provisioning did not persist one account')
    sql(owner_env, f"""
INSERT INTO provider_account_environments(provider_account_id,provider_id,environment_kind,environment_ref,upstream_identity_sha256,display_name,state,created_at_ms,updated_at_ms)
 SELECT provider_account_id,provider_id,'grok_home_v1','{auth_path.parent}','{digest(auth_path)}','Native CI synthetic account','active',1,1 FROM provider_accounts WHERE account_key='ci-account';
UPDATE provider_account_credential_heads SET refresh_strategy='reauth_only', next_refresh_at_ms=NULL;
GRANT USAGE,CREATE ON SCHEMA public TO {second};
GRANT USAGE ON SCHEMA public TO {reader};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO {reader};
ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA public GRANT SELECT ON TABLES TO {reader};
ALTER DEFAULT PRIVILEGES FOR ROLE {second} IN SCHEMA public GRANT SELECT ON TABLES TO {reader};
SET ROLE {second};
CREATE TABLE public.ci_recovery_probe(id integer PRIMARY KEY, payload text NOT NULL);
INSERT INTO public.ci_recovery_probe VALUES(1,'baseline-preserved');
CREATE SEQUENCE public.ci_recovery_sequence START 41;
CREATE VIEW public.ci_recovery_view AS SELECT * FROM public.ci_recovery_probe;
CREATE FUNCTION public.ci_recovery_value() RETURNS text LANGUAGE sql AS 'SELECT ''baseline-function''::text';
REVOKE ALL ON FUNCTION public.ci_recovery_value() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ci_recovery_value() TO {reader};
RESET ROLE;
""")
    write(STATE / 'artifacts/ci-original.bin', 'baseline-artifact\n')
    os.chown(STATE / 'artifacts/ci-original.bin', service.pw_uid, service.pw_gid)
    env_file(CONFIG / 'app.env', application)
    env_file(CONFIG / 'executors/ci.grok.env', executor)
    env_file(CONFIG / 'admin.env', {'GATEWAY_BASE_URL': 'http://127.0.0.1:8787',
        'ADMIN_CONSOLE_ORIGIN': 'http://127.0.0.1:3010', 'ADMIN_CONSOLE_CLIENT_ID': 'ai-image-factory-admin-bff'})
    policy = {'AIF_UPDATE_GITHUB_REPOSITORY': 'fixture/native', 'AIF_RELEASE_TARGET': TARGET,
              'AIF_RELEASE_METADATA_PATH': str(ROOT / 'current/release.json'), 'AIF_UPDATE_APPLY_ENABLED': 'false'}
    env_file(CONFIG / 'update-policy.env', policy)
    install_fixtures(candidate, args.candidate_bundle, args.candidate_manifest, owner_env)
    fixture = STATE / 'updater/fixture'
    diagnostics['fixture'] = fixture
    write(fixture / 'reader-role', reader)
    updater = {'AIF_RELEASE_ROOT': str(ROOT), 'AIF_UPDATE_JOURNAL_ROOT': str(STATE / 'updater'),
        'AIF_UPDATER_DATABASE_URL': dsn, 'AIF_MIGRATOR_DATABASE_URL': dsn,
        'GATEWAY_DATABASE_SCHEMA': 'public', 'GATEWAY_ARTIFACT_ROOT': str(STATE / 'artifacts'),
        'AIF_BACKUP_ROOT': str(STATE / 'backups'), 'AIF_UPDATE_GH_EXECUTABLE': str(LIB / 'ci-gh'),
        'AIF_UPDATE_ATTESTATION_WORKFLOW': '.github/workflows/release.yml',
        'AIF_UPDATE_POLL_INTERVAL_MS': '500', 'AIF_UPDATE_LEASE_MS': '300000',
        'AIF_UPDATE_ADMISSION_CLOSE_HOOK': str(LIB / 'ci-admission-close'),
        'AIF_UPDATE_ADMISSION_OPEN_HOOK': str(LIB / 'ci-admission-open'),
        'AIF_UPDATE_VERIFY_HOOK': str(LIB / 'ci-verify'),
        'AIF_UPDATE_RECOVER_HOOK': str(LIB / 'ci-recover')}
    for name in ('quiesce', 'resume', 'backup', 'activate'):
        updater['AIF_UPDATE_' + name.upper() + '_HOOK'] = str(LIB / 'hooks' / name)
    env_file(CONFIG / 'updater.env', updater)
    run(['systemctl', 'daemon-reload'])
    run(['systemctl', 'enable', PREFIX + 'executord@ci.grok.service', PREFIX + 'workerd@ci.grok.service'])
    run(['systemctl', 'start', 'aif-native-admission.service', PREFIX + 'updater.service'])
    baseline_updater_before = updater_identity(baseline_updater_hash, 'false', require_safe_mounts=False)
    run([LIB / 'hooks/start-processes'], env={'PATH': '/usr/sbin:/usr/bin:/sbin:/bin',
        'AIF_UPDATE_START_MODE': 'direct'}, timeout=180)
    wait_http(8787, '/readyz')
    wait_http(3010, '/login')
    _, original_http = authenticated_acceptance(password, account_id)
    run([LIB / 'hooks/verify'], timeout=180)
    stable_application_pids = {name: run(['systemctl', 'show', PREFIX + name + '.service', '-p', 'MainPID', '--value']).stdout.strip()
                               for name in ('gateway', 'admin')}
    progress('Original baseline is serving; Apply=false, stopping old updater and preparing exactly five hooks/three units')
    prepared, host_preparation = prepare_recovery_host_files(args.candidate_bundle, candidate, owner_env, installed_host_files)
    installed_host_files.update(prepared)
    units = unit_evidence()
    require(not (UNITS / (PREFIX + 'segmentd.service')).exists(), 'segmentd is outside recovery preparation')
    for name, pid in stable_application_pids.items():
        require(run(['systemctl', 'show', PREFIX + name + '.service', '-p', 'MainPID', '--value']).stdout.strip() == pid,
                'host preparation restarted the live baseline application')
    run([LIB / 'hooks/verify'], timeout=180)
    run(['systemctl', 'start', PREFIX + 'updater.service'])
    baseline_updater_prepared = updater_identity(baseline_updater_hash, 'false')
    write(output / 'systemd-effective.json', json.dumps(units, indent=2))
    write(output / 'host-hook-provenance.json', json.dumps({'baseline_manifest_sha256': digest(args.baseline_manifest),
        'candidate_manifest_sha256': digest(args.candidate_manifest), 'baseline_installed': baseline_host_files,
        'codex_startup_dependency': codex_dependency,
        'installed': installed_host_files, 'host_preparation': host_preparation,
        'baseline_updater_before': baseline_updater_before, 'baseline_updater_prepared': baseline_updater_prepared}, indent=2))
    # Enabling Apply is a separate, explicit CI acceptance phase. Restart the
    # old gateway so its startup-read policy matches the daemon, not a stale UI.
    policy['AIF_UPDATE_APPLY_ENABLED'] = 'true'
    env_file(CONFIG / 'update-policy.env', policy)
    run(['systemctl', 'restart', PREFIX + 'updater.service', PREFIX + 'gateway.service'])
    wait_http(8787, '/readyz')
    baseline_updater_apply = updater_identity(baseline_updater_hash, 'true')
    runtime_hook_environment = updater_hook_environment()
    token, initial_http = authenticated_acceptance(password, account_id)
    reader_receipts = {'initial': run([LIB / 'hooks/verify-admin-reader']).stdout.strip()}
    require(http(8788, '/healthz')[0] == 200, 'initial real admission endpoint is not open')
    initial_security, initial_artifacts = security_snapshot(owner_env), artifact_snapshot()
    initial_probe = sql(owner_env, 'SELECT row_to_json(t) FROM public.ci_recovery_probe t ORDER BY id;')
    account_query = "SELECT json_build_array(provider_account_id,account_key,provider_id,credential_ref,credential_revision,credential_auth_sha256) FROM provider_accounts WHERE account_key='ci-account';"
    initial_account = sql(owner_env, account_query)
    sequence_query = """
SELECT json_build_object(
  'seqstart', s.seqstart, 'seqincrement', s.seqincrement,
  'seqmax', s.seqmax, 'seqmin', s.seqmin,
  'seqcache', s.seqcache, 'seqcycle', s.seqcycle,
  'last_value', q.last_value, 'is_called', q.is_called)
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN pg_sequence s ON s.seqrelid=c.oid
CROSS JOIN public.ci_recovery_sequence q
WHERE n.nspname='public' AND c.relname='ci_recovery_sequence' AND c.relkind='S';
"""
    initial_sequence = sql(owner_env, sequence_query)
    write(output / 'initial-security.json', json.dumps(initial_security, indent=2))
    check_id = enqueue(token, 'check')
    wait_command(owner_env, check_id, {'succeeded'})
    progress('Baseline authenticated API/BFF and reader passed; running real Apply with two explicit fault points')
    write(fixture / 'inject-validation-failure', 'once\n')
    failure_id = enqueue(token, 'apply', candidate['release_version'])
    diagnostics['failure_command'] = failure_id
    failed_state = wait_command(owner_env, failure_id, {'restore_required'})
    require((fixture / 'fault-evidence.json').is_file() and (fixture / 'recovery-fault-observed').is_file(),
            'expected post-validation and first-recovery fault points were not reached')
    require(int(sql(owner_env, 'SELECT max(version) FROM _sqlx_migrations;')) == candidate['target_schema_version'],
            'failure scenario did not execute genuine candidate schema migration')
    require(sql(owner_env, 'SELECT payload FROM public.ci_recovery_probe;') == 'candidate-corruption'
            and artifact_snapshot() != initial_artifacts, 'injected data/artifact fault did not happen')
    require(http(8788, '/healthz')[0] == 503, 'failed update did not retain closed real admission')
    descriptor = STATE / 'updater/recovery' / (failure_id + '.json')
    require(descriptor.is_file() and not descriptor.is_symlink()
            and descriptor.stat().st_mode & 0o077 == 0, 'protected recovery descriptor is missing')
    descriptor_digest = digest(descriptor)
    progress('Candidate migration and validation reached; restore_required proved, starting native systemd recovery instance')
    recovery_unit = PREFIX + 'updater-recover@' + failure_id + '.service'
    run(['systemctl', 'start', recovery_unit], timeout=600)
    units = unit_evidence(failure_id)
    restored_state = wait_command(owner_env, failure_id, {'restored'}, timeout=60)
    require(restored_state['epoch'] > failed_state['epoch'], 'native recover did not fence with newer lease epoch')
    require((ROOT / 'current').resolve().name == baseline['release_version'], 'old release pointer not recovered')
    require(int(sql(owner_env, 'SELECT max(version) FROM _sqlx_migrations;')) == baseline['target_schema_version'],
            'native recovery did not restore old migration ledger')
    restored_security = security_snapshot(owner_env)
    require(restored_security == initial_security, 'owner/ACL/default ACL/extension catalog mismatch after native recovery')
    require(artifact_snapshot() == initial_artifacts, 'artifact file/digest equivalence failed')
    require(sql(owner_env, 'SELECT row_to_json(t) FROM public.ci_recovery_probe t ORDER BY id;') == initial_probe,
            'synthetic business rows were not restored')
    require(sql(owner_env, account_query) == initial_account and sql(owner_env, sequence_query) == initial_sequence,
            'stable provider account fields or sequence state were not restored')
    require(sql(pg_environment(reader_dsn), 'SELECT payload FROM public.ci_recovery_view; SELECT public.ci_recovery_value();')
            == 'baseline-preserved\nbaseline-function', 'restored reader view/function access failed')
    require(sql(owner_env, "SELECT to_regclass('public.ci_candidate_only') IS NULL;") == 't',
            'candidate-only DDL survived recovery')
    require(not descriptor.exists(), 'native recovery descriptor remains')
    reader_receipts['restored'] = run([LIB / 'hooks/verify-admin-reader']).stdout.strip()
    token, restored_http = authenticated_acceptance(password, account_id)
    require(http(8788, '/healthz')[0] == 200, 'native recovery did not reopen real admission')
    write(output / 'restored-security.json', json.dumps(restored_security, indent=2))
    progress('Recovery catalog/data/artifact equivalence and authenticated reads passed; running positive real Apply')
    run(['systemctl', 'start', PREFIX + 'updater.service'])
    baseline_updater_positive = updater_identity(baseline_updater_hash, 'true')
    check_id = enqueue(token, 'check')
    wait_command(owner_env, check_id, {'succeeded'})
    success_id = enqueue(token, 'apply', candidate['release_version'])
    successful_state = wait_command(owner_env, success_id, {'succeeded'})
    require((ROOT / 'current').resolve().name == candidate['release_version'], 'positive upgrade pointer mismatch')
    require(int(sql(owner_env, 'SELECT max(version) FROM _sqlx_migrations;')) == candidate['target_schema_version'],
            'positive upgrade schema ledger mismatch')
    reader_receipts['upgraded'] = run([LIB / 'hooks/verify-admin-reader']).stdout.strip()
    _, successful_http = authenticated_acceptance(password, account_id)
    require(artifact_snapshot() == initial_artifacts and http(8788, '/healthz')[0] == 200,
            'positive upgrade changed artifact content or kept admission closed')
    # Only after the application passes does the existing supervisor-controlled
    # helper replace the fixed updater. This is not a precondition for recovery:
    # both Apply paths above ran the original baseline binary from /proc/PID/exe.
    progress('Application upgrade verified with baseline updater; disabling Apply before the existing fixed-binary helper')
    policy['AIF_UPDATE_APPLY_ENABLED'] = 'false'
    env_file(CONFIG / 'update-policy.env', policy)
    run(['systemctl', 'restart', PREFIX + 'gateway.service'])
    wait_http(8787, '/readyz')
    require(sql(owner_env, "SELECT count(*) FROM platform_update_commands WHERE status IN ('queued','running','restoring','restore_required');") == '0',
            'fixed updater helper must not interrupt an active command')
    application_before_helper = {name: run(['systemctl', 'show', PREFIX + name + '.service', '-p', 'MainPID', '--value']).stdout.strip()
                                 for name in ('gateway', 'admin')}
    helper_result = run([LIB / 'upgrade-updater', ROOT / 'current/bin/updated'],
        env={'PATH': '/usr/sbin:/usr/bin:/sbin:/bin'}, timeout=180)
    candidate_updater_after_helper = updater_identity(candidate_updater_hash, 'false')
    require(digest(LIB / '.updated.previous') == baseline_updater_hash,
            'native helper did not retain the original fixed binary')
    for name, pid in application_before_helper.items():
        require(run(['systemctl', 'show', PREFIX + name + '.service', '-p', 'MainPID', '--value']).stdout.strip() == pid,
                'fixed updater helper restarted the verified application')
    _, helper_http = authenticated_acceptance(password, account_id)
    require(http(8788, '/healthz')[0] == 200 and artifact_snapshot() == initial_artifacts,
            'fixed updater helper changed admission or artifact content')
    installed_host_files['bin/updated'] = {'destination': str(LIB / 'updated'), 'sha256': candidate_updater_hash}
    journal = (STATE / 'updater/events.jsonl').read_text()
    require(failure_id in journal and success_id in journal, 'missing native updater journal identity')
    exported_events, _ = bounded_updater_events(STATE / 'updater/events.jsonl')
    exported_ids = {event.get('command_id') for event in exported_events}
    require(failure_id in exported_ids and success_id in exported_ids,
            'bounded updater evidence omitted a required command identity')
    write(output / 'updater-events.jsonl', ''.join(
        json.dumps(event, separators=(',', ':'), ensure_ascii=False) + '\n'
        for event in exported_events))
    write(output / 'verify-runs.jsonl', (fixture / 'verify-runs.jsonl').read_text())
    write(output / 'reader-gates.json', json.dumps(reader_receipts, indent=2))
    write(output / 'systemd-effective.json', json.dumps(units, indent=2))
    write(output / 'host-hook-provenance.json', json.dumps({'baseline_manifest_sha256': digest(args.baseline_manifest),
        'candidate_manifest_sha256': digest(args.candidate_manifest), 'baseline_installed': baseline_host_files,
        'codex_startup_dependency': codex_dependency,
        'host_preparation': host_preparation, 'candidate_preparation_files': sorted(RECOVERY_HOST_FILES),
        'baseline_updater_before': baseline_updater_before, 'baseline_updater_prepared': baseline_updater_prepared,
        'baseline_updater_apply': baseline_updater_apply, 'baseline_updater_positive': baseline_updater_positive,
        'candidate_updater_after_helper': candidate_updater_after_helper,
        'updater_helper_receipt': helper_result.stdout.strip(), 'previous_updater_sha256': digest(LIB / '.updated.previous'),
        'installed': installed_host_files, 'runtime_hook_environment': runtime_hook_environment,
        'fault_wrapper_delegation': {'ci-verify': str(LIB / 'hooks/verify'), 'ci-recover': str(LIB / 'hooks/recover')}}, indent=2))
    write(output / 'artifact-equivalence.json', json.dumps(initial_artifacts, indent=2))
    for item in installed_host_files.values():
        require(digest(Path(item['destination'])) == item['sha256'], 'fixed host code changed during rehearsal')
    summary = {'passed': True, 'elapsed_seconds': round(time.monotonic() - started, 3),
        'postgres': sql(admin, 'SHOW server_version;'),
        'baseline': {'version': baseline['release_version'], 'sha': baseline['commit_sha'],
                     'schema': baseline['target_schema_version']},
        'candidate': {'version': candidate['release_version'], 'sha': candidate['commit_sha'],
                      'schema': candidate['target_schema_version']},
        'failure_command': failure_id, 'failure_state': failed_state, 'restored_state': restored_state,
        'protected_descriptor_sha256': descriptor_digest,
        'positive_command': success_id, 'positive_state': successful_state,
        'http': {'original_baseline': original_http, 'prepared_baseline': initial_http,
                 'restored': restored_http, 'upgraded': successful_http, 'after_updater_helper': helper_http},
        'boundaries': ['GitHub release transport and signature/attestation responses are explicit fixtures, not cryptographic acceptance',
                       'Real loopback admission proxy, not production nginx',
                       'Authenticated page shells and real BFF data with explicit Secure Cookie replay, not browser-rendered data/TLS policy acceptance',
                       'Recovery preparation is exactly five hooks/three units with baseline updater retained; segmentd and segmentation host wiring are not enabled',
                       'Candidate fixed updater is installed only after application verification through the existing native helper, with Apply disabled',
                       'Real pinned Codex CLI is only a startup dependency; no credentials or inference and no segmentation capability claim',
                       'Synthetic DB/roles/identity/account/artifacts only; no provider/model request'],
        'data_equivalence_scope': ['ci_recovery_probe row', 'provider account stable identity/credential fields',
                                   'ci_recovery_sequence state', 'ci_recovery_view and function read results',
                                   'all artifact files present in this synthetic fixture'],
        'security_catalog_items': len(initial_security), 'artifact_files': len(initial_artifacts)}
    write(output / 'summary.json', json.dumps(summary, indent=2))
    print(json.dumps({'passed': True, 'output': str(output), 'failure_command': failure_id,
                      'positive_command': success_id, 'schema': [baseline['target_schema_version'], candidate['target_schema_version']]}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ('baseline-bundle', 'baseline-manifest', 'candidate-bundle', 'candidate-manifest', 'codex-runtime', 'output-dir'):
        parser.add_argument('--' + option, type=Path, required=True)
    args = parser.parse_args()
    admin, output = preflight(args)
    diagnostics = {}
    original_error = None
    try:
        execute(args, admin, output, diagnostics)
    except Exception as error:
        original_error = error
        try:
            write(output / 'summary.json', json.dumps(
                {'passed': False, 'error': sanitized(str(error))}, indent=2))
        except Exception:
            pass
        raise
    finally:
        # The disposable VM is discarded by Actions. Preserve its synthetic
        # state on disk for the job's diagnostics, never recursively delete a
        # broad path or export database dumps, EnvironmentFiles or private keys.
        collect_recovery_evidence_best_effort(output, diagnostics, original_error)
        try:
            result = run(['journalctl', '--no-pager', '-n', '800', '-u', 'ai-image-factory-*'],
                         timeout=20, check=False)
            write(output / 'services.log', sanitized(result.stdout + result.stderr))
        except Exception:
            pass
        # The workflow exports only its explicit sanitized evidence allowlist
        # using sudo, never this private directory recursively.
        try:
            run(['systemctl', 'stop', PREFIX + 'updater.service', PREFIX + 'processes.target',
                 'aif-native-admission.service'], timeout=180, check=False)
        except Exception:
            pass


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('native recovery rehearsal FAILED: ' + sanitized(str(error)), file=sys.stderr)
        sys.exit(1)
