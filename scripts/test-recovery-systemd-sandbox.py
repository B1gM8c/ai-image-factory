"""Check recovery sandbox contracts; --runtime also exercises real Linux systemd.

Runtime mode requires root on a disposable Linux systemd host. It only creates
randomly named temporary directories and transient units, not Factory services.
"""
import errno
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import uuid


ROOT = Path(__file__).resolve().parents[1]
STATE = '/var/lib/ai-image-factory'
UNITS = {
    'ai-image-factory-updater.service': '/usr/libexec/ai-image-factory/updated',
    'ai-image-factory-updater-recover@.service': '/usr/libexec/ai-image-factory/updated recover %i',
    'ai-image-factory-recovery-gate.service': '/usr/libexec/ai-image-factory/updated recover-pending',
}
HARDENING = {
    'UMask': '0077',
    'NoNewPrivileges': 'yes',
    'PrivateDevices': 'yes',
    'PrivateTmp': 'yes',
    'ProtectControlGroups': 'yes',
    'ProtectHome': 'yes',
    'ProtectKernelLogs': 'yes',
    'ProtectKernelModules': 'yes',
    'ProtectKernelTunables': 'yes',
    'ProtectSystem': 'strict',
    'ReadOnlyPaths': '/usr/libexec/ai-image-factory',
    'RestrictAddressFamilies': 'AF_UNIX AF_INET AF_INET6',
    'RestrictSUIDSGID': 'yes',
    'SystemCallArchitectures': 'native',
}


def directives(source, section):
    values = {}
    current = None
    for line in source.splitlines():
        line = line.strip()
        if line.startswith('[') and line.endswith(']'):
            current = line[1:-1]
        elif current == section and line and not line.startswith(('#', ';')):
            key, value = line.split('=', 1)
            values.setdefault(key.strip(), []).append(value.strip())
    return values


def validate(name, source):
    service = directives(source, 'Service')
    unit = directives(source, 'Unit')
    expected = dict(HARDENING, User='root', Group='root', ExecStart=UNITS[name])
    for key, value in expected.items():
        if service.get(key) != [value]:
            raise ValueError(f'{name}: changed {key} contract')
    if service.get('ReadWritePaths') != ['/opt/ai-image-factory', STATE]:
        raise ValueError(f'{name}: require exact state parent write allow-list')
    if service.get('BindPaths') or service.get('BindReadOnlyPaths'):
        raise ValueError(f'{name}: unexpected explicit bind mounts')
    if name == 'ai-image-factory-updater-recover@.service':
        if unit.get('Conflicts') != ['ai-image-factory-updater.service']:
            raise ValueError('manual recovery must conflict with the updater daemon')
        if 'ai-image-factory-updater.service' not in ' '.join(unit.get('After', [])).split():
            raise ValueError('manual recovery must wait for the updater daemon to stop')
    if name != 'ai-image-factory-updater.service':
        if unit.get('OnFailure') != ['ai-image-factory-recovery-failed.service']:
            raise ValueError('recovery failure must remain fail closed')


class SandboxContractTests(unittest.TestCase):
    def test_all_three_units(self):
        for name in UNITS:
            with self.subTest(unit=name):
                validate(name, (ROOT / 'deploy/systemd' / name).read_text())

    def test_reject_missing_parent_redundant_paths_and_broad_write_access(self):
        for replacement in (
            f'ReadWritePaths={STATE}/artifacts',
            f'ReadWritePaths={STATE}\nReadWritePaths={STATE}/artifacts',
            'ReadWritePaths=/var/lib',
            'ReadWritePaths=/',
        ):
            for name in UNITS:
                with self.subTest(unit=name, replacement=replacement):
                    source = (ROOT / 'deploy/systemd' / name).read_text()
                    with self.assertRaises(ValueError):
                        validate(name, source.replace(f'ReadWritePaths={STATE}\n', replacement + '\n'))

    def test_reject_explicit_bind_mounts(self):
        for name in UNITS:
            source = (ROOT / 'deploy/systemd' / name).read_text()
            for key in ('BindPaths', 'BindReadOnlyPaths'):
                with self.subTest(unit=name, directive=key), self.assertRaises(ValueError):
                    validate(name, source.replace('[Service]\n', f'[Service]\n{key}={STATE}/artifacts\n'))

    def test_reject_weakened_hardening(self):
        for name in UNITS:
            source = (ROOT / 'deploy/systemd' / name).read_text()
            for key, value in HARDENING.items():
                with self.subTest(unit=name, directive=key):
                    with self.assertRaises(ValueError):
                        validate(name, source.replace(f'{key}={value}', f'{key}='))

    def test_reject_changed_entrypoint_and_conflict(self):
        for name, entrypoint in UNITS.items():
            source = (ROOT / 'deploy/systemd' / name).read_text()
            with self.subTest(unit=name), self.assertRaises(ValueError):
                validate(name, source.replace(entrypoint, '/bin/true'))
        name = 'ai-image-factory-updater-recover@.service'
        source = (ROOT / 'deploy/systemd' / name).read_text()
        with self.assertRaises(ValueError):
            validate(name, source.replace('Conflicts=ai-image-factory-updater.service', 'Conflicts='))

    def test_reject_unordered_daemon_stop(self):
        name = 'ai-image-factory-updater-recover@.service'
        source = (ROOT / 'deploy/systemd' / name).read_text()
        with self.assertRaisesRegex(ValueError, 'wait for the updater daemon'):
            validate(name, source.replace(
                'After=network-online.target ai-image-factory-updater.service',
                'After=network-online.target'))


# The same filesystem operations as recover: sibling mktemp, old-root rename,
# staged-root rename, and cleanup. OSError is reported without hiding errno.
PROBE = '''import errno, json, os, pathlib, shutil, stat, sys, tempfile
parent = pathlib.Path(sys.argv[1])
artifacts = parent / "artifacts"
# All fixture paths are generated below /var/lib without mountinfo escapes.
mountinfo = []
artifact_mounts = []
for line in pathlib.Path("/proc/self/mountinfo").read_text().splitlines():
    mountpoint = pathlib.Path(line.split()[4])
    if mountpoint == parent or mountpoint in parent.parents or parent in mountpoint.parents:
        mountinfo.append(line)
    if mountpoint == artifacts or artifacts in mountpoint.parents:
        artifact_mounts.append(str(mountpoint))
status = pathlib.Path("/proc/self/status").read_text()
outcome = {
    "stage": "hardening", "errno": None, "mountinfo": mountinfo,
    "artifact_mounts": artifact_mounts, "parent_mode": stat.S_IMODE(parent.stat().st_mode),
    "no_new_privileges": "NoNewPrivs:\\t1" in status, "readonly_errno": None,
}
try:
    assert outcome["no_new_privileges"], "NoNewPrivileges was not applied"
    try:
        pathlib.Path(sys.argv[2], "must-stay-read-only").write_text("unexpected")
    except OSError as error:
        outcome["readonly_errno"] = error.errno
        assert error.errno == errno.EROFS, error
    else:
        raise AssertionError("out-of-scope directory became writable")
    outcome["stage"] = "mktemp"
    try:
        restored = pathlib.Path(tempfile.mkdtemp(prefix=".artifacts.restore.", dir=parent))
        (restored / "marker").write_text("restored")
        outcome["stage"] = "rename_old"
        os.rename(artifacts, parent / ".artifacts.before-recovery")
        outcome["stage"] = "rename_restored"
        os.rename(restored, artifacts)
        shutil.rmtree(parent / ".artifacts.before-recovery")
        outcome.update(stage="completed", errno=0)
    except OSError as error:
        outcome["errno"] = error.errno
finally:
    print(json.dumps(outcome), flush=True)
'''


def runtime():
    if sys.platform != 'linux' or os.geteuid() != 0 or not Path('/run/systemd/system').is_dir():
        raise RuntimeError('--runtime requires root on a disposable Linux host running systemd')
    runner = shutil.which('systemd-run')
    if not runner:
        raise RuntimeError('systemd-run is required')
    with tempfile.TemporaryDirectory(prefix='aif-recovery-sandbox-', dir='/var/lib') as directory:
        root = Path(directory)
        readonly = root / 'trusted-hooks'
        readonly.mkdir()
        script = root / 'probe.py'
        script.write_text(PROBE)
        # systemd v255 namespace.c:drop_nop folds same-mode ReadWritePaths
        # descendants; explicit BindPaths entries are not folded.
        for case, expected_stage, expected_errno, expected_child_mount in (
            ('child-only', 'mktemp', errno.EROFS, True),
            ('parent-and-child', 'completed', 0, False),
            ('parent-only', 'completed', 0, False),
            ('parent-and-bind-child', 'rename_old', errno.EBUSY, True),
        ):
            parent = root / case
            parent.mkdir(mode=0o750)
            parent.chmod(0o750)
            artifacts = parent / 'artifacts'
            artifacts.mkdir(mode=0o750)
            artifacts.chmod(0o750)
            (artifacts / 'marker').write_text('original')
            paths = [artifacts] if case == 'child-only' else [parent]
            if case == 'parent-and-child':
                paths.append(artifacts)
            properties = dict(HARDENING, ReadOnlyPaths=str(readonly), User='root', Group='root')
            if case == 'parent-and-bind-child':
                properties['BindPaths'] = f'{artifacts}:{artifacts}:norbind'
            command = [runner, '--quiet', '--wait', '--pipe', '--collect',
                '--unit=aif-recovery-sandbox-' + uuid.uuid4().hex,
                '--property=Type=oneshot', '--property=TimeoutStartSec=30s']
            command.extend('--property=' + key + '=' + value for key, value in properties.items())
            command.append('--property=ReadWritePaths=' + ' '.join(map(str, paths)))
            result = subprocess.run(command + [sys.executable, str(script), str(parent), str(readonly)],
                capture_output=True, text=True, timeout=45)
            if result.returncode:
                raise AssertionError(f'{case}: probe exited {result.returncode}; '
                    f'stdout={result.stdout}; stderr={result.stderr}')
            outcome = json.loads(result.stdout.strip())
            print(json.dumps({'runtime': 'real-systemd', 'case': case, **outcome}), flush=True)
            if (outcome['stage'], outcome['errno']) != (expected_stage, expected_errno):
                raise AssertionError(f'{case}: {outcome}, expected {expected_stage}/{expected_errno}')
            expected_mounts = [str(artifacts)] if expected_child_mount else []
            if outcome['artifact_mounts'] != expected_mounts:
                raise AssertionError(f'{case}: unexpected artifact mounts: {outcome}')
            if not outcome['no_new_privileges'] or outcome['readonly_errno'] != errno.EROFS:
                raise AssertionError(f'{case}: hardening was not applied: {outcome}')
            if outcome['parent_mode'] != 0o750 or parent.stat().st_mode & 0o777 != 0o750:
                raise AssertionError(f'{case}: state parent mode changed: {outcome}')
            marker = 'restored' if expected_stage == 'completed' else 'original'
            if (artifacts / 'marker').read_text() != marker:
                raise AssertionError(f'{case}: artifact marker mismatch')
        runtime_lock_handoff(runner, root)


def runtime_lock_handoff(runner, root):
    # Reproduce the real host-lock race with a deliberately slow daemon stop.
    # Transient fixtures never start or stop installed Factory services.
    daemon_program = '''import fcntl, pathlib, sys, time
with open(sys.argv[1], "w") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    pathlib.Path(sys.argv[2]).touch()
    while True:
        time.sleep(1)
'''
    recovery_program = '''import fcntl, sys
with open(sys.argv[1], "a") as lock:
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("host-lock-busy", flush=True)
        sys.exit(73)
    print("host-lock-acquired", flush=True)
'''
    unit = directives((ROOT / 'deploy/systemd/ai-image-factory-updater-recover@.service').read_text(), 'Unit')
    for ordered in (False, True):
        suffix = uuid.uuid4().hex
        daemon = 'aif-lock-daemon-' + suffix + '.service'
        recovery = 'aif-lock-recover-' + suffix + '.service'
        lock = root / (suffix + '.lock')
        ready = root / (suffix + '.ready')
        try:
            subprocess.run([runner, '--quiet', '--collect', '--unit=' + daemon,
                '--property=ExecStop=/bin/sleep 2', '--property=TimeoutStopSec=10s',
                sys.executable, '-c', daemon_program, str(lock), str(ready)],
                check=True, capture_output=True, text=True, timeout=15)
            deadline = time.monotonic() + 10
            while not ready.exists():
                if time.monotonic() >= deadline:
                    raise AssertionError('fixture daemon did not acquire the host lock')
                time.sleep(0.05)
            command = [runner, '--quiet', '--wait', '--pipe', '--collect',
                '--unit=' + recovery, '--property=Type=oneshot',
                '--property=TimeoutStartSec=15s', '--property=Conflicts=' + daemon]
            if ordered:
                # Use the shipped unit's ordering, replacing only the fixture name.
                after = ' '.join(unit.get('After', [])).replace('ai-image-factory-updater.service', daemon)
                command.append('--property=After=' + after)
            result = subprocess.run(command + [sys.executable, '-c', recovery_program, str(lock)],
                capture_output=True, text=True, timeout=25)
            marker = 'host-lock-acquired' if ordered else 'host-lock-busy'
            if (result.returncode == 0) != ordered or marker not in result.stdout:
                raise AssertionError(f'lock handoff ordered={ordered}: {result.returncode}; '
                    f'stdout={result.stdout}; stderr={result.stderr}')
            print(json.dumps({'runtime': 'real-systemd', 'case': 'host-lock-handoff',
                'ordered': ordered, 'result': marker}), flush=True)
        finally:
            subprocess.run(['systemctl', 'stop', recovery, daemon],
                capture_output=True, timeout=20)


if __name__ == '__main__':
    run_runtime = '--runtime' in sys.argv
    if run_runtime:
        sys.argv.remove('--runtime')
    result = unittest.main(exit=False)
    if not result.result.wasSuccessful():
        sys.exit(1)
    if run_runtime:
        runtime()
