"""Synthetic maintenance failure drills, never Factory services or credentials.

Runtime mode is restricted to a GitHub-hosted Linux runner with real systemd.
Every service and filesystem target is randomly named and owned by this test.
This tests recovery primitives, not an approved production maintenance script.
"""
import argparse
import fcntl
import os
from pathlib import Path
import signal
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid


def command(*args, timeout=10, check=True):
    return subprocess.run(args, capture_output=True, text=True,
                          timeout=timeout, check=check)


def wait_for(predicate, seconds=10):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(.05)
    raise AssertionError('bounded condition did not converge')


def unit_state(unit):
    return command('systemctl', 'show', unit, '-p', 'ActiveState',
                   '--value').stdout.strip()


def require_unit(unit):
    if not unit.startswith('aif-drill-') or not unit.endswith('.service'):
        raise ValueError('only synthetic drill units are allowed')


def recover(root, admin):
    require_unit(admin)
    with (root / 'recovery.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if (root / 'recovered').exists():
            return
        # Do not reverse an ongoing stop operation with a blind start.
        wait_for(lambda: unit_state(admin) not in ('deactivating', 'activating'))
        command('systemctl', 'start', admin)
        wait_for(lambda: unit_state(admin) == 'active')
        if (root / 'unhealthy').exists():
            (root / 'admission').write_text('closed')
            (root / 'failed-closed').touch()
            return
        with (root / 'restore-count').open('a') as out:
            out.write('restore\n')
        (root / 'admission').write_text('open')
        (root / 'recovered').touch()


def actor(role, root, admin, controller):
    require_unit(admin)
    require_unit(controller)
    if not root.name.startswith('aif-drill-') or not root.is_dir():
        raise ValueError('invalid synthetic fixture')
    if role == 'slow-admin':
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        (root / 'admin-ready').touch()
        while True:
            time.sleep(1)
    if role == 'controller':
        (root / 'admission').write_text('closed')
        # Mark responsibility before the first destructive operation.
        (root / 'admin-stop-attempted').touch()
        command('systemctl', 'stop', admin)
        (root / 'controller-ready').touch()
        while True:
            time.sleep(1)
    if role == 'guardian':
        wait_for(lambda: (root / 'controller-ready').exists())
        (root / ('guardian-ready-' + str(os.getpid()))).touch()
        wait_for(lambda: unit_state(controller) in ('inactive', 'failed'))
        recover(root, admin)


class StaticTests(unittest.TestCase):
    def test_production_unit_names_are_refused(self):
        for name in ('ai-image-factory-admin.service', 'nginx.service', ''):
            with self.assertRaises(ValueError):
                require_unit(name)

    def test_all_connections_must_close_even_with_empty_queues(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0)); listener.listen()
        peers = [socket.create_connection(listener.getsockname()) for _ in range(2)]
        accepted = [listener.accept()[0] for _ in peers]
        try:
            peers[0].close()
            accepted[0].settimeout(1)
            self.assertEqual(accepted[0].recv(1), b'')
            # Closing the admin-like connection leaves the other one active.
            accepted[1].settimeout(.05)
            with self.assertRaises(socket.timeout):
                accepted[1].recv(1)
            peers[1].close()
            self.assertEqual(accepted[1].recv(1), b'')
        finally:
            for sock in peers + accepted + [listener]: sock.close()


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='aif-drill-', dir='/var/tmp')
        self.root = Path(self.temp.name)
        self.prefix = 'aif-drill-' + uuid.uuid4().hex
        self.units = []
        self.admin = self.prefix + '-admin.service'
        self.controller = self.prefix + '-controller.service'

    def transient(self, unit, args, properties=()):
        require_unit(unit)
        self.units.append(unit)
        command('systemd-run', '--quiet', '--unit=' + unit,
                '--setenv=GITHUB_ACTIONS=true',
                '--property=RuntimeMaxSec=45', '--property=TimeoutStopSec=2',
                *properties, *args)

    def actor_args(self, role):
        return [sys.executable, str(Path(__file__).resolve()), '--actor', role,
                '--root', str(self.root), '--admin', self.admin,
                '--controller', self.controller]

    def start_admin(self, slow=False):
        args = self.actor_args('slow-admin') if slow else ['/bin/sleep', 'infinity']
        # A runtime-linked fixture remains restartable after an explicit stop;
        # a stopped transient unit may otherwise be garbage-collected.
        self.units.append(self.admin)
        (self.root / self.admin).write_text(
            '[Unit]\nDescription=Synthetic maintenance admin\n[Service]\n'
            'Environment=GITHUB_ACTIONS=true\nTimeoutStopSec=2\n'
            'ExecStart=' + shlex.join(args) + '\n')
        command('systemctl', 'link', '--runtime', str(self.root / self.admin))
        command('systemctl', 'daemon-reload')
        command('systemctl', 'start', self.admin)
        wait_for(lambda: unit_state(self.admin) == 'active')
        if slow: wait_for(lambda: (self.root / 'admin-ready').exists())

    def tearDown(self):
        for unit in reversed(self.units):
            command('systemctl', 'stop', unit, timeout=8, check=False)
            command('systemctl', 'reset-failed', unit, check=False)
        link = Path('/run/systemd/system') / self.admin
        if link.is_symlink() and link.resolve() == self.root / self.admin:
            link.unlink()
            command('systemctl', 'daemon-reload')
        self.temp.cleanup()

    def test_stop_client_timeout_does_not_cancel_systemd_stop(self):
        self.start_admin(slow=True)
        with self.assertRaises(subprocess.TimeoutExpired):
            command('systemctl', 'stop', self.admin, timeout=.15)
        self.assertEqual(unit_state(self.admin), 'deactivating')
        # Let the owned stop job finish under its configured 2s deadline.
        # Do not cancel arbitrary jobs or submit a conflicting start.
        wait_for(lambda: unit_state(self.admin) in ('inactive', 'failed'))
        self.assertEqual(command('systemctl', 'show', self.admin, '-p',
                                 'MainPID', '--value').stdout.strip(), '0')
        command('systemctl', 'start', self.admin)
        wait_for(lambda: unit_state(self.admin) == 'active')

    def kill_controller_with_guardians(self, unhealthy=False):
        self.start_admin()
        if unhealthy: (self.root / 'unhealthy').touch()
        self.transient(self.controller, self.actor_args('controller'))
        wait_for(lambda: (self.root / 'controller-ready').exists())
        self.assertTrue((self.root / 'admin-stop-attempted').exists())
        for index in range(2):
            self.transient(self.prefix + f'-guardian{index}.service',
                           self.actor_args('guardian'))
        wait_for(lambda: len(list(self.root.glob('guardian-ready-*'))) == 2)
        command('systemctl', 'kill', '--signal=KILL', '--kill-whom=all', self.controller)
        marker = 'failed-closed' if unhealthy else 'recovered'
        wait_for(lambda: (self.root / marker).exists())
        wait_for(lambda: all(unit_state(u) in ('inactive', 'failed')
                            for u in self.units if '-guardian' in u))

    def test_independent_guardians_survive_sigkill_and_recover_once(self):
        self.kill_controller_with_guardians()
        self.assertEqual((self.root / 'admission').read_text(), 'open')
        self.assertEqual((self.root / 'restore-count').read_text(), 'restore\n')
        self.assertEqual(unit_state(self.admin), 'active')

    def test_unhealthy_recovery_never_opens(self):
        self.kill_controller_with_guardians(unhealthy=True)
        self.assertEqual((self.root / 'admission').read_text(), 'closed')
        self.assertFalse((self.root / 'recovered').exists())
        self.assertFalse((self.root / 'restore-count').exists())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime', action='store_true')
    parser.add_argument('--actor', choices=['slow-admin', 'controller', 'guardian'])
    parser.add_argument('--root'); parser.add_argument('--admin'); parser.add_argument('--controller')
    args = parser.parse_args()
    if args.runtime or args.actor:
        if not (sys.platform == 'linux' and os.geteuid() == 0
                and os.environ.get('GITHUB_ACTIONS') == 'true'
                and Path('/proc/1/comm').read_text().strip() == 'systemd'):
            raise SystemExit('requires disposable GitHub Linux systemd runner')
    if args.actor:
        actor(args.actor, Path(args.root), args.admin, args.controller)
    else:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(StaticTests)
        if args.runtime:
            suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(RuntimeTests))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        raise SystemExit(not result.wasSuccessful())
