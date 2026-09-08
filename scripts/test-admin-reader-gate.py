"""Real PostgreSQL permission matrix; mock only systemd/proc/HTTP, never SQL.

Use a disposable loopback superuser TEST_DATABASE_URL. This creates and removes
only a uniquely named test database/login. It is not a full release rehearsal.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from urllib.parse import quote, unquote, urlsplit, urlunsplit
import uuid


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'requires disposable loopback PostgreSQL')
class AdminReaderGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_url = os.environ['TEST_DATABASE_URL']
        parts = urlsplit(cls.base_url)
        if parts.hostname not in ('127.0.0.1', 'localhost') or parts.query or parts.fragment:
            raise AssertionError('test accepts a plain loopback PostgreSQL URL only')
        cls.database = 'aif_reader_gate_' + uuid.uuid4().hex
        cls.reader = cls.database + '_reader'
        cls.password = uuid.uuid4().hex + ":@/?#%'"
        cls.test_url = urlunsplit(parts._replace(path='/' + cls.database))
        host_port = parts.hostname + ':' + str(parts.port or 5432)
        cls.reader_url = urlunsplit(parts._replace(
            netloc=cls.reader + ':' + quote(cls.password, safe='') + '@' + host_port,
            path='/' + cls.database))
        cls.psql = shutil.which(os.environ.get('TEST_PSQL', 'psql'))
        if not cls.psql:
            raise AssertionError('psql is required')
        cls.sql('CREATE ROLE ' + cls.reader + " LOGIN NOINHERIT PASSWORD '"
                + cls.password.replace("'", "''") + "'", cls.base_url)
        try:
            cls.sql('CREATE DATABASE ' + cls.database, cls.base_url)
        except Exception:
            cls.sql('DROP ROLE ' + cls.reader, cls.base_url)
            raise
        print('PostgreSQL permission tests: ' + cls.sql('SHOW server_version').stdout.strip())

    @classmethod
    def tearDownClass(cls):
        cls.sql('DROP DATABASE ' + cls.database, cls.base_url)
        cls.sql('DROP ROLE ' + cls.reader, cls.base_url)

    @classmethod
    def sql(cls, sql, database=None):
        parts = urlsplit(database or cls.test_url)
        result = subprocess.run([cls.psql, '-X', '-w', '-qAt', '--set=ON_ERROR_STOP=1'],
            input=sql, env={'PATH': os.environ['PATH'], 'PGDATABASE': unquote(parts.path[1:]),
                            'PGHOST': parts.hostname, 'PGPORT': str(parts.port or 5432),
                            'PGUSER': unquote(parts.username or ''),
                            'PGPASSWORD': unquote(parts.password or ''),
                            'PGCONNECT_TIMEOUT': '5'},
            capture_output=True, text=True, timeout=15, check=False)
        if result.returncode:
            raise AssertionError('fixture SQL failed, exit=' + str(result.returncode))
        return result

    def setUp(self):
        self.sql('DROP SCHEMA public CASCADE; CREATE SCHEMA public;'
            'CREATE TABLE public.jobs(job_id uuid);'
            "INSERT INTO public.jobs VALUES ('00000000-0000-0000-0000-000000000001');"
            'CREATE TABLE public.usage_events(billing_metric text);'
            'CREATE TABLE public.provider_accounts(provider_account_id uuid);'
            'CREATE TABLE public.another_admin_table(id integer);'
            "CREATE FUNCTION public.reject_update() RETURNS trigger LANGUAGE plpgsql AS $$"
            "BEGIN RAISE EXCEPTION 'gate executed an update'; END $$;"
            'CREATE TRIGGER reject_update BEFORE UPDATE ON public.jobs FOR EACH STATEMENT '
            'EXECUTE FUNCTION public.reject_update();'
            'GRANT USAGE ON SCHEMA public TO ' + self.reader + ';'
            'GRANT SELECT ON ALL TABLES IN SCHEMA public TO ' + self.reader + ';')
        self.temp = tempfile.TemporaryDirectory(prefix='aif-admin-reader-gate-')
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.bin = self.directory / 'bin'
        self.bin.mkdir()
        self.proc = self.directory / 'proc/101'
        self.proc.mkdir(parents=True)
        self.release = self.directory / 'releases/test/bin'
        self.release.mkdir(parents=True)
        (self.release / 'gateway').touch()
        (self.proc / 'exe').symlink_to(self.release / 'gateway')
        (self.directory / 'current').symlink_to(self.release.parent)
        self.write_process_environment()
        self.executable('systemctl', '''#!/bin/sh
case "$*" in
  *--property=MainPID*) echo 101;;
  *--property=NRestarts*) echo 0;;
  'is-active --quiet '*) exit 0;;
  'is-enabled --quiet '*) exit 1;;
  'list-dependencies '*) exit 0;;
  *) exit 2;;
esac
''')
        self.executable('curl', '#!/bin/sh\nexit 0\n')
        self.executable('sleep', '#!/bin/sh\nexit 0\n')
        self.executable('runtime-gate', '#!/bin/sh\nexit 0\n')
        # This wrapper records no values from the private environment; real
        # psql is always executed. It proves the DSN is absent from argv.
        self.executable('psql', '#!' + shutil.which('python3') + '\n'
            'import json, os, sys\n'
            'from pathlib import Path\n'
            'Path(' + repr(str(self.directory / 'psql-invocations.jsonl')) + ')'
            '.open("a").write(json.dumps({"argv": sys.argv[1:], '
            '"environment_keys": sorted(os.environ), '
            '"inherited_password": os.environ.get("PGPASSWORD") == "must-not-inherit"}) + "\\n")\n'
            'os.execv(' + repr(self.psql) + ', [' + repr(self.psql) + '] + sys.argv[1:])\n')
        self.gate()  # Every negative case must first prove its real SQL baseline.

    def executable(self, name, content):
        path = self.bin / name
        path.write_text(content)
        path.chmod(0o755)
        return path

    def write_process_environment(self, dsn=None, suffix=b''):
        path = self.proc / 'environ'
        path.write_bytes(b'GATEWAY_ADMIN_READ_DATABASE_URL=' +
                         (dsn or self.reader_url).encode() + b'\0' + suffix)
        path.chmod(0o600)

    def gate(self, expected=True, scope=None):
        env = dict(os.environ,
            AIF_VERIFY_COMMAND_PATH=str(self.bin) + ':' + os.environ['PATH'],
            AIF_VERIFY_PROC_ROOT=str(self.directory / 'proc'),
            AIF_VERIFY_CURRENT_RELEASE_LINK=str(self.directory / 'current'),
            AIF_VERIFY_GATEWAY_RUNTIME_GATE=str(self.bin / 'runtime-gate'),
            AIF_VERIFY_MAX_ATTEMPTS='1', AIF_VERIFY_STABILITY_SECONDS='0',
            PGPASSWORD='must-not-inherit', GH_TOKEN='must-not-inherit')
        path = ROOT / 'deploy/hooks/verify-admin-reader'
        if scope:
            env['AIF_UPDATE_PROCESS_SCOPE'] = scope
            path = ROOT / 'deploy/hooks/verify'
        result = subprocess.run([str(path)], env=env, capture_output=True,
                                text=True, timeout=30, check=False)
        self.assertEqual(result.returncode == 0, expected, result.stdout + result.stderr)
        for secret in (self.reader_url, self.password, 'must-not-inherit'):
            self.assertNotIn(secret, result.stdout + result.stderr)
        receipt = self.directory / 'psql-invocations.jsonl'
        if receipt.exists():
            for line in receipt.read_text().splitlines():
                invocation = json.loads(line)
                self.assertNotIn(self.reader_url, str(invocation['argv']))
                self.assertFalse(invocation['inherited_password'])
                self.assertNotIn('GH_TOKEN', invocation['environment_keys'])
        self.assertEqual(self.sql('SELECT job_id FROM public.jobs').stdout.strip(),
                         '00000000-0000-0000-0000-000000000001')
        return result

    def test_real_direct_login_reads_empty_and_nonempty_tables_and_denies_write(self):
        self.gate()

    def test_revoke_schema_usage_fails(self):
        self.sql('REVOKE USAGE ON SCHEMA public FROM ' + self.reader)
        self.gate(False)

    def test_revoke_select_on_representative_table_fails(self):
        self.sql('REVOKE SELECT ON public.provider_accounts FROM ' + self.reader)
        self.gate(False)

    def test_revoke_select_on_other_admin_table_fails(self):
        self.sql('REVOKE SELECT ON public.another_admin_table FROM ' + self.reader)
        self.gate(False)

    def test_each_effective_write_privilege_fails(self):
        for privilege in ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER'):
            with self.subTest(privilege=privilege):
                self.sql('GRANT ' + privilege + ' ON public.another_admin_table TO ' + self.reader)
                self.gate(False)
                self.sql('REVOKE ' + privilege + ' ON public.another_admin_table FROM ' + self.reader)

    def test_column_only_update_fails(self):
        self.sql('GRANT UPDATE(id) ON public.another_admin_table TO ' + self.reader)
        self.gate(False)

    def test_column_select_grant_option_fails(self):
        self.sql('GRANT SELECT(job_id) ON public.jobs TO ' + self.reader + ' WITH GRANT OPTION')
        direct = self.sql("SELECT has_table_privilege(current_user, 'public.jobs', "
            "'SELECT WITH GRANT OPTION'), has_any_column_privilege(current_user, "
            "'public.jobs', 'SELECT WITH GRANT OPTION')", self.reader_url)
        self.assertEqual(direct.stdout.strip(), 'f|t')
        self.gate(False)

    def test_schema_create_and_grant_options_fail(self):
        for privilege in ('CREATE', 'USAGE WITH GRANT OPTION'):
            with self.subTest(privilege=privilege):
                self.sql('GRANT ' + privilege + ' ON SCHEMA public TO ' + self.reader
                         if privilege == 'CREATE' else
                         'GRANT USAGE ON SCHEMA public TO ' + self.reader + ' WITH GRANT OPTION')
                self.gate(False)
                self.sql('REVOKE ALL ON SCHEMA public FROM ' + self.reader + ';'
                         'GRANT USAGE ON SCHEMA public TO ' + self.reader)

    def test_writer_login_is_rejected_even_with_session_read_only_on(self):
        self.write_process_environment(self.test_url + '?options=-cdefault_transaction_read_only%3Don')
        self.gate(False)

    def test_reader_session_read_only_is_overridden_for_permission_proof(self):
        self.write_process_environment(self.reader_url + '?options=-cdefault_transaction_read_only%3Don')
        self.gate()

    def test_set_role_does_not_substitute_for_direct_reader_login(self):
        self.write_process_environment(self.test_url + '?options=-crole%3D' + self.reader)
        self.gate(False)

    def test_noinherit_reader_with_settable_writer_role_fails(self):
        writer = self.database + '_writer'
        self.sql('CREATE ROLE ' + writer + ' NOLOGIN;'
                 'GRANT USAGE ON SCHEMA public TO ' + writer + ';'
                 'GRANT UPDATE ON public.jobs TO ' + writer + ';'
                 'GRANT ' + writer + ' TO ' + self.reader)
        self.addCleanup(self.sql, 'REVOKE ' + writer + ' FROM ' + self.reader + ';'
                        'REVOKE UPDATE ON public.jobs FROM ' + writer + ';'
                        'REVOKE USAGE ON SCHEMA public FROM ' + writer + ';DROP ROLE ' + writer)
        direct = self.sql("SELECT NOT rolinherit, "
            "has_table_privilege(current_user, 'public.jobs', 'UPDATE'), "
            "pg_has_role(current_user, '" + writer + "', 'SET') "
            "FROM pg_roles WHERE rolname = current_user", self.reader_url)
        self.assertEqual(direct.stdout.strip(), 't|f|t')
        switched = self.sql('BEGIN READ ONLY; SET LOCAL ROLE ' + writer + ';'
            "SELECT has_table_privilege(current_user, 'public.jobs', 'UPDATE');ROLLBACK;",
            self.reader_url)
        self.assertEqual(switched.stdout.strip(), 't')
        self.gate(False)

    def test_missing_dedicated_login_fails(self):
        (self.proc / 'environ').write_bytes(b'DATABASE_URL=' + self.test_url.encode() + b'\0')
        self.gate(False)

    def test_libpq_errors_never_expose_connection_string(self):
        marker = uuid.uuid4().hex
        self.write_process_environment('postgresql://reader:' + marker + '@127.0.0.1:1/db')
        result = self.gate(False)
        self.assertNotIn(marker, result.stdout + result.stderr)

    def test_validation_and_full_both_run_reader_twice(self):
        for scope in ('validation', 'full'):
            with self.subTest(scope=scope):
                result = self.gate(scope=scope)
                self.assertEqual(result.stdout.count('admin reader gate passed:'), 2)

    def test_validation_and_full_reject_reader_failure_even_when_http_is_healthy(self):
        self.sql('REVOKE SELECT ON public.jobs FROM ' + self.reader)
        for scope in ('validation', 'full'):
            with self.subTest(scope=scope):
                self.gate(False, scope=scope)


if __name__ == '__main__':
    unittest.main()
