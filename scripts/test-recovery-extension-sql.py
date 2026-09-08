"""Exact backup SQL round trips on disposable loopback PostgreSQL, not systemd.

Set TEST_DATABASE_URL (CREATE DATABASE/ROLE rights), TEST_PSQL, TEST_PG_DUMP,
and optionally TEST_PG_RESTORE (defaults to pg_dump's sibling).
Native updater, artifacts and authenticated HTTP are a separate rehearsal gate.
"""
from pathlib import Path
import hashlib
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from urllib.parse import urlsplit, urlunsplit
import uuid


ROOT = Path(__file__).resolve().parents[1]
FORMAT = '-- AIF database recovery format 2: owners-acls-extension'


def hook_function(hook, name):
    source = (ROOT / 'deploy/hooks' / hook).read_text()
    match = re.search(r'^' + re.escape(name) + r'\(\) \{\n.*?^\}', source, re.M | re.S)
    if not match:
        raise AssertionError(f'{hook}: {name} missing')
    return match.group()


class BackupFormatTests(unittest.TestCase):
    def validate(self, first_line=FORMAT, metadata=None):
        command = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
        if metadata is None:
            metadata = f'DATABASE_RECOVERY_FORMAT=2\nUPDATE_COMMAND_ID={command}\nRELEASE_VERSION=v-test\nDATABASE_SCHEMA=public\n'
        with tempfile.TemporaryDirectory(prefix='aif-backup-format-') as directory:
            path = Path(directory)
            (path / 'database.sql').write_text(first_line + '\n')
            (path / 'recovery.env').write_text(metadata)
            digest = hashlib.sha256(metadata.encode()).hexdigest()
            (path / 'SHA256SUMS').write_text(digest + '  recovery.env\n')
            return subprocess.run(['bash', '-c', 'set -euo pipefail\n'
                + hook_function('recover', 'validate_database_backup')
                + '\nAIF_UPDATE_COMMAND_ID="$1"\nAIF_UPDATE_RELEASE_VERSION=v-test\nGATEWAY_DATABASE_SCHEMA=public\nvalidate_database_backup "$2"',
                'test', command, directory], capture_output=True, text=True, check=False)

    def test_new_format_and_identity_accepted(self):
        result = self.validate()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_legacy_or_incorrect_identity_rejected(self):
        for line, metadata in [('DROP SCHEMA public CASCADE;', None),
                (FORMAT, 'DATABASE_RECOVERY_FORMAT=1\n'),
                (FORMAT, 'DATABASE_RECOVERY_FORMAT=2\nUPDATE_COMMAND_ID=other\nDATABASE_SCHEMA=public\n')]:
            with self.subTest(line=line, metadata=metadata):
                self.assertNotEqual(self.validate(line, metadata).returncode, 0)

    def test_preflight_and_transaction_guards_remain(self):
        source = (ROOT / 'deploy/hooks/recover').read_text()
        self.assertLess(source.index('validate_database_backup "$RECOVERY_DIRECTORY"'),
            source.index('systemctl stop ai-image-factory-processes.target'))
        for guard in ('--set=ON_ERROR_STOP=1', '--single-transaction',
                'sha256sum --check SHA256SUMS', "WHERE command_id = :'update_command_id'::UUID"):
            self.assertIn(guard, source)
        self.assertIn("RAISE EXCEPTION 'restored database did not contain", source)


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'requires disposable loopback PostgreSQL')
class PostgreSQLRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        parts = urlsplit(os.environ['TEST_DATABASE_URL'])
        if parts.hostname not in ('127.0.0.1', 'localhost'):
            raise AssertionError('recovery tests accept loopback PostgreSQL only')
        cls.base_url = os.environ['TEST_DATABASE_URL']
        cls.psql = os.environ.get('TEST_PSQL', 'psql')
        cls.pg_dump = os.environ.get('TEST_PG_DUMP', 'pg_dump')
        cls.pg_restore = os.environ.get('TEST_PG_RESTORE', str(Path(cls.pg_dump).with_name('pg_restore')))
        cls.prefix = 'aif_rc_' + uuid.uuid4().hex[:16]
        cls.role_password = uuid.uuid4().hex
        cls.roles = {name: cls.prefix + '_' + name for name in ('owner', 'object_owner', 'reader', 'migrator')}
        cls.database = cls.prefix + '_db'
        cls.test_url = urlunsplit(parts._replace(path='/' + cls.database))
        result = cls.sql('\n'.join('CREATE ROLE ' + role + " LOGIN PASSWORD '" + cls.role_password + "';" for role in cls.roles.values()), database=cls.base_url)
        if result.returncode:
            raise AssertionError(result.stderr)
        result = cls.sql('CREATE DATABASE ' + cls.database + ' OWNER ' + cls.roles['owner'], database=cls.base_url, transactional=False)
        if result.returncode:
            raise AssertionError(result.stderr)

    @classmethod
    def tearDownClass(cls):
        result = cls.sql('DROP DATABASE ' + cls.database, database=cls.base_url, transactional=False)
        if result.returncode:
            raise AssertionError(result.stderr)
        result = cls.sql('\n'.join('DROP ROLE ' + role + ';' for role in cls.roles.values()), database=cls.base_url)
        if result.returncode:
            raise AssertionError(result.stderr)

    @classmethod
    def environment(cls, database=None, role=None):
        parts = urlsplit(database or cls.test_url)
        return dict(os.environ, PGHOST=parts.hostname, PGPORT=str(parts.port or 5432),
            PGUSER=role or parts.username or '', PGPASSWORD=cls.role_password if role else parts.password or '', PGDATABASE=parts.path.lstrip('/'))

    @classmethod
    def sql(cls, sql, database=None, role=None, transactional=True):
        arguments = [cls.psql, '-X', '-qAt', '--set=ON_ERROR_STOP=1', '--set=VERBOSITY=verbose', '--set=database_schema=app']
        if transactional:
            arguments.append('--single-transaction')
        # A regular stdin file avoids a pipe deadlock between a large dump and
        # PostgreSQL's verbose DROP CASCADE notices on small-buffer platforms.
        with tempfile.TemporaryFile(mode='w+') as source:
            source.write(sql)
            source.seek(0)
            return subprocess.run(arguments, stdin=source, env=cls.environment(database, role),
                capture_output=True, text=True, check=False, timeout=60)

    def setUp(self):
        r = self.roles
        result = self.sql(f'''
DROP SCHEMA IF EXISTS app CASCADE;
GRANT {r['owner']}, {r['object_owner']} TO {r['migrator']};
ALTER DEFAULT PRIVILEGES FOR ROLE {r['owner']} REVOKE SELECT ON TABLES FROM {r['reader']};
CREATE SCHEMA app AUTHORIZATION {r['owner']};
SET ROLE {r['owner']};
CREATE EXTENSION btree_gist WITH SCHEMA app VERSION '1.7';
REVOKE ALL ON SCHEMA app FROM PUBLIC;
GRANT USAGE ON SCHEMA app TO {r['reader']};
GRANT USAGE, CREATE ON SCHEMA app TO {r['object_owner']};
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO {r['reader']};
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT USAGE ON SEQUENCES TO {r['reader']};
CREATE TABLE app.jobs(id bigint GENERATED BY DEFAULT AS IDENTITY, marker text, uid uuid,
    EXCLUDE USING gist(uid WITH =));
INSERT INTO app.jobs(marker,uid) VALUES ('original', '01234567-89ab-cdef-0123-456789abcdef');
REVOKE ALL ON app.jobs FROM PUBLIC;
CREATE FUNCTION app.probe(integer) RETURNS integer LANGUAGE sql AS 'SELECT $1';
REVOKE ALL ON FUNCTION app.probe(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.probe(integer) TO {r['reader']};
CREATE TYPE app.colour AS ENUM ('red', 'blue');
REVOKE ALL ON TYPE app.colour FROM PUBLIC;
GRANT USAGE ON TYPE app.colour TO {r['reader']};
SET ROLE {r['object_owner']};
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO {r['reader']};
CREATE TABLE app.other_owner(id integer, secret text);
INSERT INTO app.other_owner VALUES (9, 'data-preserved');
REVOKE SELECT ON app.other_owner FROM {r['reader']};
GRANT SELECT(id) ON app.other_owner TO {r['reader']};
RESET ROLE;
''')
        self.assertEqual(result.returncode, 0, result.stderr)

    def security(self):
        extracted = subprocess.run(['bash', '-c', hook_function('backup', 'database_security_query') + '\ndatabase_security_query'], capture_output=True, text=True, check=True)
        result = self.sql('SET search_path = pg_catalog;\n' + extracted.stdout + ';')
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def backup(self, role=None, schema='app'):
        with tempfile.TemporaryDirectory(prefix='aif-backup-sql-') as directory:
            path = Path(directory)
            for executable, target in [('psql', self.psql), ('pg_dump', self.pg_dump), ('pg_restore', self.pg_restore)]:
                (path / executable).symlink_to(Path(target).resolve() if '/' in target else Path(shutil.which(target)))
            functions = '\n'.join(hook_function('backup', name) for name in
                ('database_dependency_guard', 'database_security_query', 'prepare_database_sql'))
            role = role or self.roles['migrator']
            parts = urlsplit(self.test_url)
            url = urlunsplit(parts._replace(netloc=f'{role}:{self.role_password}@{parts.hostname}:{parts.port or 5432}'))
            environment = dict(self.environment(role=role), DATABASE_URL=url, GATEWAY_DATABASE_SCHEMA=schema, PATH=directory + ':' + os.environ['PATH'])
            result = subprocess.run(['bash', '-c', 'set -euo pipefail\n' + functions + '\nprepare_database_sql "$1"', 'test', directory], env=environment, capture_output=True, text=True, check=False, timeout=60)
            sql_file = path / 'database.sql'
            return result, sql_file.read_text() if sql_file.exists() else None

    def test_native_dump_omits_extension_identity_then_exact_round_trip(self):
        native = subprocess.run([self.pg_dump, '--schema=app', '--extension=btree_gist', '--quote-all-identifiers', '--schema-only'], env=self.environment(), capture_output=True, text=True, check=True).stdout
        self.assertIn('CREATE EXTENSION IF NOT EXISTS "btree_gist" WITH SCHEMA "app";', native)
        before = self.security()
        result, image = self.backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('VERSION \'1.7\'', image)
        self.assertIn('ALTER DEFAULT PRIVILEGES FOR ROLE "' + self.roles['owner'] + '"', image)
        self.assertRegex(image, 'OWNER TO "?' + self.roles['object_owner'] + '"?;')
        self.assertEqual(image.splitlines()[0], FORMAT)
        self.assertEqual(self.sql("UPDATE app.jobs SET marker='after-upgrade'").returncode, 0)
        restored = self.sql(image, role=self.roles['migrator'])
        self.assertEqual(restored.returncode, 0, restored.stderr)
        self.assertEqual(self.security(), before)
        self.assertEqual(self.sql('SELECT marker FROM app.jobs').stdout.strip(), 'original')
        self.assertEqual(self.sql('SELECT id, secret FROM app.other_owner').stdout.strip(), '9|data-preserved')
        self.assertEqual(self.sql("SELECT nextval('app.jobs_id_seq')").stdout.strip(), '2')
        reader = self.roles['reader']
        read = self.sql('SELECT session_user = current_user, marker FROM app.jobs', role=reader)
        self.assertEqual(read.returncode, 0, read.stderr)
        self.assertEqual(read.stdout.strip(), 't|original')
        write = self.sql('SET transaction_read_only=off; EXPLAIN UPDATE app.jobs SET marker=marker WHERE false;', role=reader)
        self.assertEqual(write.returncode, 3)
        self.assertIn('42501', write.stderr)
        created = self.sql('CREATE TABLE app.future(id integer);', role=self.roles['owner'])
        self.assertEqual(created.returncode, 0, created.stderr)
        self.assertEqual(self.sql('SELECT count(*) FROM app.future', role=reader).returncode, 0)

    def test_acl_loss_and_injected_sql_fault_each_roll_back(self):
        before = self.security()
        result, image = self.backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        missing_acl = image.replace('GRANT SELECT ON TABLE "app"."jobs" TO "' + self.roles['reader'] + '";', '')
        self.assertNotEqual(missing_acl, image)
        for broken in (missing_acl, image + '\nSELECT deliberately_missing_recovery_function();\n'):
            with self.subTest(fault='acl' if broken == missing_acl else 'sql'):
                failed = self.sql(broken, role=self.roles['migrator'])
                self.assertEqual(failed.returncode, 3, failed.stderr)
                self.assertIn('identity mismatch' if broken == missing_acl else 'deliberately_missing_recovery_function', failed.stderr)
                self.assertEqual(self.security(), before)
                self.assertEqual(self.sql('SELECT marker FROM app.jobs').stdout.strip(), 'original')

    def test_extension_owner_permission_failure_does_not_publish_sql(self):
        result = self.sql('REVOKE ' + self.roles['owner'] + ' FROM ' + self.roles['migrator'])
        self.assertEqual(result.returncode, 0, result.stderr)
        result, image = self.backup()
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(image)

    def test_object_owner_permission_failure_does_not_publish_sql(self):
        r = self.roles
        result = self.sql(f"REVOKE {r['object_owner']} FROM {r['migrator']}; GRANT SELECT ON app.other_owner TO {r['migrator']};")
        self.assertEqual(result.returncode, 0, result.stderr)
        result, image = self.backup()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('cannot restore saved schema/object owners', result.stderr)
        self.assertIsNone(image)

    def test_non_owner_grantor_requiring_session_authorization_is_rejected(self):
        r = self.roles
        result = self.sql(f'''SET ROLE {r['owner']};
GRANT SELECT ON app.jobs TO {r['object_owner']} WITH GRANT OPTION;
SET ROLE {r['object_owner']}; GRANT SELECT ON app.jobs TO {r['reader']};''')
        self.assertEqual(result.returncode, 0, result.stderr)
        result, image = self.backup()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('unavailable session authorization', result.stderr)
        self.assertIsNone(image)

    def test_extension_owner_without_database_create_is_rejected(self):
        r = self.roles
        result = self.sql(f"ALTER DATABASE {self.database} OWNER TO {urlsplit(self.base_url).username}; GRANT CREATE ON DATABASE {self.database} TO {r['migrator']};")
        self.assertEqual(result.returncode, 0, result.stderr)
        try:
            result, image = self.backup()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('cannot restore the saved extension owner', result.stderr)
            self.assertIsNone(image)
        finally:
            self.assertEqual(self.sql(f'ALTER DATABASE {self.database} OWNER TO {r["owner"]};').returncode, 0)

    def test_modified_extension_member_acl_without_owner_rights_is_rejected(self):
        result = self.sql(f"REVOKE EXECUTE ON FUNCTION app.gbt_int4_consistent(internal, integer, smallint, oid, internal) FROM PUBLIC; GRANT EXECUTE ON FUNCTION app.gbt_int4_consistent(internal, integer, smallint, oid, internal) TO {self.roles['reader']};")
        self.assertEqual(result.returncode, 0, result.stderr)
        result, image = self.backup()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('cannot restore modified extension member ACLs', result.stderr)
        self.assertIsNone(image)

    def test_global_default_acl_is_explicitly_unsupported_and_cannot_silently_drift(self):
        result, image = self.backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        r = self.roles
        self.assertEqual(self.sql(f"ALTER DEFAULT PRIVILEGES FOR ROLE {r['owner']} GRANT SELECT ON TABLES TO {r['reader']};").returncode, 0)
        before = self.security()
        result, rejected_image = self.backup()
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(rejected_image)
        failed = self.sql(image, role=r['migrator'])
        self.assertEqual(failed.returncode, 3, failed.stderr)
        self.assertIn('identity mismatch', failed.stderr)
        self.assertEqual(self.security(), before, 'global ACL drift must not commit a partial restore')

    def test_additional_extension_and_missing_dependency_fail_closed(self):
        self.assertEqual(self.sql('CREATE EXTENSION hstore WITH SCHEMA app').returncode, 0)
        result, image = self.backup()
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(image)
        self.assertEqual(self.sql('DROP EXTENSION hstore; DROP EXTENSION btree_gist CASCADE;').returncode, 0)
        result, image = self.backup()
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(image)

    def test_quoted_schema_backup_preserves_identity(self):
        self.assertEqual(self.sql('ALTER SCHEMA app RENAME TO "Case_Schema";').returncode, 0)
        try:
            result, image = self.backup(schema='Case_Schema')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('CREATE SCHEMA "Case_Schema";', image)
            self.assertIn('WITH SCHEMA "Case_Schema" VERSION', image)
            # Supply the same caller schema variable as the native recover hook.
            restored = self.sql("\\set database_schema Case_Schema\n" + image, role=self.roles['migrator'])
            self.assertEqual(restored.returncode, 0, restored.stderr)
        finally:
            self.assertEqual(self.sql('ALTER SCHEMA "Case_Schema" RENAME TO app;').returncode, 0)

    def test_copy_data_that_looks_like_sql_metadata_is_not_rewritten(self):
        payloads = ['-- Name: SCHEMA "app"; Type: ACL; Schema: -; Owner: pretend',
            'ALTER SCHEMA app OWNER TO pretend;',
            'CREATE EXTENSION IF NOT EXISTS "btree_gist" WITH SCHEMA "app";']
        statements = 'CREATE TABLE app.text_payload(value text);\n' + '\n'.join(
            "INSERT INTO app.text_payload VALUES ('" + value.replace("'", "''") + "');" for value in payloads)
        self.assertEqual(self.sql(statements, role=self.roles['owner']).returncode, 0)
        result, image = self.backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        restored = self.sql(image, role=self.roles['migrator'])
        self.assertEqual(restored.returncode, 0, restored.stderr)
        self.assertEqual(self.sql('SELECT value FROM app.text_payload ORDER BY value').stdout.splitlines(), sorted(payloads))

    def test_external_view_and_foreign_key_are_rejected_before_backup_or_restore(self):
        for external in ('CREATE VIEW audit.v AS SELECT marker FROM app.jobs;',
                'CREATE TABLE audit.child(id bigint REFERENCES app.jobs(id));'):
            with self.subTest(external=external):
                self.assertEqual(self.sql('ALTER TABLE app.jobs ADD PRIMARY KEY(id);').returncode, 0)
                result, image = self.backup()
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.sql('CREATE SCHEMA audit; ' + external).returncode, 0)
                try:
                    rejected, rejected_image = self.backup()
                    self.assertNotEqual(rejected.returncode, 0)
                    self.assertIn('external or unsupported dependencies', rejected.stderr)
                    self.assertIsNone(rejected_image)
                    failed = self.sql(image, role=self.roles['migrator'])
                    self.assertEqual(failed.returncode, 3, failed.stderr)
                    self.assertIn('external or unsupported dependencies', failed.stderr)
                    self.assertEqual(self.sql("SELECT count(*) FROM pg_class WHERE relnamespace='audit'::regnamespace AND relkind IN ('v','r')").stdout.strip(), '1')
                    self.assertEqual(self.sql('SELECT marker FROM app.jobs').stdout.strip(), 'original')
                finally:
                    self.assertEqual(self.sql('DROP SCHEMA audit CASCADE; ALTER TABLE app.jobs DROP CONSTRAINT jobs_pkey;').returncode, 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
