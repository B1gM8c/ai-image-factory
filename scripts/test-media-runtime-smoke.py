"""Real-PG/process readiness and configuration rollback smoke; no model calls.

Set TEST_DATABASE_URL to an explicit disposable PostgreSQL database containing
`test` in its name. Build gateway, segmentd and factoryctl first. Only a fresh
UUID schema is mutated; evidence/logs are retained in a private temp directory.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin-dir', type=Path, required=True)
    parser.add_argument('--psql', default='psql')
    args = parser.parse_args()
    binaries = args.bin_dir.resolve(strict=True)
    dsn = os.environ['TEST_DATABASE_URL']
    parsed = urllib.parse.urlparse(dsn)
    if parsed.scheme not in ('postgres', 'postgresql') or 'test' not in parsed.path:
        raise SystemExit('Explicit disposable test database required')
    os.umask(0o077)
    evidence = Path(tempfile.mkdtemp(prefix='aif-media-runtime-'))
    schema = 'media_runtime_' + uuid.uuid4().hex
    # Bind a currently free local port; a bind race safely fails gateway startup.
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    env = {key: os.environ[key] for key in ('PATH', 'HOME', 'LANG', 'TMPDIR')
           if key in os.environ}
    env.update({'DATABASE_URL': dsn, 'GATEWAY_DATABASE_SCHEMA': schema,
        'GATEWAY_BIND': '127.0.0.1:' + str(port),
        'GATEWAY_IMAGES_GENERATION_CONTRACT': 'customer-pricing-v4',
        'GATEWAY_IDENTITY_ENABLED': 'false', 'GATEWAY_API_TOKEN': secrets.token_hex(32),
        'GATEWAY_API_KEY_PEPPERS': '1:' + secrets.token_hex(32),
        'GATEWAY_API_KEY_CURRENT_PEPPER_VERSION': '1',
        'GATEWAY_WEBHOOK_SIGNING_KEYS': '1:' + secrets.token_hex(32),
        'GATEWAY_WEBHOOK_CURRENT_SIGNING_KEY_VERSION': '1',
        'GATEWAY_ARTIFACT_ROOT': str(evidence / 'artifacts'),
        'GATEWAY_PROVIDER_HOME_ROOT': str(evidence / 'provider-homes'),
        'GATEWAY_MANAGED_CODEX_EXECUTABLE': '/usr/bin/false',
        'GATEWAY_BBOX_CODEX_BIN': '/usr/bin/false',
        'GATEWAY_BBOX_CODEX_HOME': str(evidence / 'bbox-home'),
        'GATEWAY_BBOX_ENABLED': 'true', 'GATEWAY_CODEX_QUOTA_AUTO_REFRESH_INTERVAL_SECONDS': '5',
        'RUST_LOG': 'info'})
    for directory in ('bbox-home', 'artifacts', 'provider-homes'):
        (evidence / directory).mkdir(mode=0o700)
    keyfile = evidence / 'api-key'
    keyfile.write_text(env['GATEWAY_API_TOKEN'])
    base = 'http://127.0.0.1:' + str(port)
    repo = Path(__file__).resolve().parents[1]
    processes = []
    stopped = set()
    report = {'schema': schema, 'evidence': str(evidence), 'checks': {}}
    pg_env = dict(env, PGDATABASE=urllib.parse.unquote(parsed.path.lstrip('/')),
        PGHOST=parsed.hostname or '127.0.0.1', PGPORT=str(parsed.port or 5432),
        PGOPTIONS='-c statement_timeout=5000')
    if parsed.username:
        pg_env['PGUSER'] = urllib.parse.unquote(parsed.username)
    if parsed.password:
        pg_env['PGPASSWORD'] = urllib.parse.unquote(parsed.password)

    def sql(statement):
        return subprocess.run([args.psql, '-X', '-A', '-t', '-v', 'ON_ERROR_STOP=1',
            '-c', statement], env=pg_env, capture_output=True, text=True, check=True).stdout.strip()

    def run(binary, *argv):
        return subprocess.run([str(binaries / binary), *argv], env=env,
                              capture_output=True, text=True, timeout=30)

    def start(binary, label):
        with (evidence / (label + '.log')).open('ab') as log:
            process = subprocess.Popen([str(binaries / binary)], env=env,
                stdout=log, stderr=log, stdin=subprocess.DEVNULL)
        processes.append(process)
        return process

    def stop(process):
        stopped.add(process.pid)
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    def get(path):
        request = urllib.request.Request(base + path,
            headers={'Authorization': 'Bearer ' + env['GATEWAY_API_TOKEN']})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=2) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def wait_until(path, expected):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if any(process.poll() is not None and process.pid not in stopped
                   for process in processes):
                raise AssertionError('Test process exited; inspect logs in ' + str(evidence))
            try:
                code, body = get(path)
                if code == expected:
                    return body
            except (urllib.error.URLError, TimeoutError):
                pass
            time.sleep(0.1)
        raise AssertionError('Runtime did not reach expected state: ' + path)

    def gate(mode):
        result = subprocess.run(['python3', str(repo / 'deploy/hooks/verify-media-segments'),
            '--base-url', base, '--api-key-file', str(keyfile),
            '--factoryctl', str(binaries / 'factoryctl'), '--source-release', mode,
            '--quota-refresh', mode], env=env, capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise AssertionError(result.stderr)
        return json.loads(result.stdout)

    sql('CREATE SCHEMA ' + schema)
    try:
        assert run('factoryctl', 'verify-migrations').returncode != 0
        report['checks']['missing_migrations_rejected'] = True
        migration = run('factoryctl', 'migrate')
        assert migration.returncode == 0, migration.stderr
        assert run('factoryctl', 'verify-migrations').returncode == 0
        env['GATEWAY_CODEX_QUOTA_AUTO_REFRESH_ENABLED'] = 'true'
        env['GATEWAY_BBOX_RELEASE_TERMINAL_SOURCES'] = 'true'
        gateway = start('gpt-image-2-gateway', 'enabled-gateway')
        wait_until('/readyz', 200)
        missing = wait_until('/v1/media/readiness', 503)
        assert missing['reason'] == 'worker_heartbeat_missing'
        report['checks']['missing_worker_rejected'] = True
        worker = start('segmentd', 'enabled-worker')
        wait_until('/v1/media/readiness', 200)
        report['checks']['enabled_gate'] = gate('enabled')
        env['GATEWAY_BBOX_RELEASE_TERMINAL_SOURCES'] = 'false'
        mixed_worker = start('segmentd', 'mixed-mode-worker')
        mixed = wait_until('/v1/media/readiness', 503)
        assert mixed['reason'] == 'worker_configuration_mismatch'
        assert mixed['source_release_enabled'] is None
        report['checks']['mixed_live_worker_modes_rejected'] = True
        stop(mixed_worker)
        sql('UPDATE ' + schema + '.media_segment_worker_heartbeats SET observed_at_ms=1 '
            'WHERE release_terminal_sources=false')
        env['GATEWAY_BBOX_MODEL'] = 'gpt-5.6-sol'
        other_analyzer = start('segmentd', 'mixed-analyzer-worker')
        mixed = wait_until('/v1/media/readiness', 503)
        assert mixed['reason'] == 'worker_configuration_mismatch'
        report['checks']['mixed_analyzer_worker_modes_rejected'] = True
        stop(other_analyzer)
        env.pop('GATEWAY_BBOX_MODEL')
        stop(worker)
        # Inject staleness only into this test's UUID schema, without waiting150s.
        sql('UPDATE ' + schema + '.media_segment_worker_heartbeats SET observed_at_ms=1')
        stale = wait_until('/v1/media/readiness', 503)
        assert stale['reason'] == 'worker_heartbeat_stale'
        report['checks']['stopped_stale_worker_rejected'] = True
        stop(gateway)
        env['GATEWAY_CODEX_QUOTA_AUTO_REFRESH_ENABLED'] = 'false'
        env['GATEWAY_BBOX_RELEASE_TERMINAL_SOURCES'] = 'false'
        gateway = start('gpt-image-2-gateway', 'rollback-gateway')
        worker = start('segmentd', 'rollback-worker')
        wait_until('/readyz', 200)
        wait_until('/v1/media/readiness', 200)
        report['checks']['config_rollback_gate'] = gate('disabled')
        counts = json.loads(sql('SELECT json_build_object(\'jobs\',(SELECT count(*) FROM '
            + schema + '.jobs),\'sidecars\',(SELECT count(*) FROM ' + schema
            + '.media_segment_results),\'quota_attempt_rows\',(SELECT count(*) FROM '
            + schema + '.provider_account_quota_refreshes))'))
        assert counts == {'jobs': 0, 'sidecars': 0, 'quota_attempt_rows': 0}
        report['checks']['no_model_or_image_work'] = counts
        report['status'] = 'passed'
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                stop(process)
        sql('DROP SCHEMA ' + schema + ' CASCADE')
        report['test_schema_removed'] = True
        (evidence / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
