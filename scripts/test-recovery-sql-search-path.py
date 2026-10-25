"""Exercise section normalization and real budget CHECKs on loopback PostgreSQL.

No pg_dump/pg_restore client is required. Fixtures use their three native SQL
section prologues, with the real migration0143 function and CHECK definitions.
"""
from pathlib import Path
import hashlib
import os
import re
import resource
import stat
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import urlsplit
import uuid


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "deploy/hooks/restore-sql-search-path"
RESET = "SELECT pg_catalog.set_config('search_path', '', false);\n"
COMMAND = "8a540ae6-e6fc-4bd4-bf97-1dfe7a87f4d1"
VERSION = "v0.1.0-20261026.hotfix.26.33d56dc"
PROLOGUE = """--
-- PostgreSQL database dump
--

-- Dumped from database version 16.10
-- Dumped by pg_dump version 16.10

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
""" + RESET + """SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

"""


def section(body, data=False):
    marker = "-- Data for Name: fixture; Type: TABLE DATA; Schema: fixture; Owner: fixture\n" if data else "-- Name: fixture; Type: TABLE; Schema: fixture; Owner: fixture\n"
    return PROLOGUE + "--\n" + marker + "--\n\n" + body


def invoke(mode, source, output, schema="fixture", extra=(), **kwargs):
    return subprocess.run([sys.executable, str(HELPER), mode, "--schema", schema,
        "--input", str(source), "--output", str(output), *extra],
        capture_output=True, text=True, timeout=30, **kwargs)


class SectionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="aif-recovery-path-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def write(self, source):
        path = self.root / (uuid.uuid4().hex + ".sql")
        path.write_text(source)
        path.chmod(0o600)
        return path

    def test_only_prologue_changes_and_output_is_private(self):
        body = "CREATE FUNCTION fixture.literal_marker() RETURNS text LANGUAGE sql AS $function$\nSELECT $value$\n" + RESET + "-- PostgreSQL database dump\n$value$;\n$function$;\nCOPY fixture.payload (value) FROM stdin;\n" + RESET + "-- Name: payload marker\n\\.\n"
        for data in (False, True):
            base = section(body, data=data)
            native18 = base.replace("-- Dumped from database version 16.10", "\\restrict NativePg18Token123\n\n-- Dumped from database version 18.3").replace("SET lock_timeout = 0;\n", "SET lock_timeout = 0;\nSET transaction_timeout = 0;\n")
            post18 = native18.replace("SET row_security = off;\n", "SET row_security = off;\nSET default_tablespace = '';\n")
            for name, text in (("pg16", base), ("pg18", native18), ("pg18-post", post18)):
                with self.subTest(data=data, native=name):
                    source = self.write(text)
                    original_sha = hashlib.sha256(source.read_bytes()).hexdigest()
                    output = self.root / (uuid.uuid4().hex + ".sql")
                    result = invoke("section", source, output)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    expected = text.replace(RESET, "SELECT pg_catalog.set_config('search_path', '\"fixture\"', false);\n", 1)
                    self.assertEqual(output.read_text(), expected)
                    self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
                    self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), original_sha)

    def test_unknown_or_ambiguous_prologue_fails_without_output(self):
        invalid = [
            section("SELECT 1;\n").replace(RESET, ""),
            section("SELECT 1;\n").replace(RESET, RESET + RESET),
            section("SELECT 1;\n").replace("SET statement_timeout = 0;", "SELECT 1;"),
            section("SELECT 1;\n").replace("-- PostgreSQL database dump", "-- unknown dump", 1),
        ]
        for text in invalid:
            with self.subTest(prefix=text[:50]):
                output = self.root / (uuid.uuid4().hex + ".sql")
                result = invoke("section", self.write(text), output)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())

    def test_legacy_requires_real_pinned_image_and_identity(self):
        source = self.write(section("SELECT 1;\n"))
        for command, version in ((COMMAND, VERSION), (str(uuid.uuid4()), VERSION), (COMMAND, "v-other")):
            with self.subTest(command=command, version=version):
                output = self.root / (uuid.uuid4().hex + ".sql")
                result = invoke("legacy", source, output, extra=("--command-id", command, "--release-version", version))
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())

    def test_native_empty_section_is_exact_and_unknown_footer_is_rejected(self):
        for key in (None, "NativeKey123"):
            prefix = PROLOGUE
            if key:
                prefix = prefix.replace("SET statement_timeout", "\\restrict " + key + "\n\nSET statement_timeout", 1)
            footer = "--\n-- PostgreSQL database dump complete\n--\n\n"
            if key:
                footer += "\\unrestrict " + key + "\n\n"
            text = prefix + footer
            output = self.root / (uuid.uuid4().hex + ".sql")
            result = invoke("section", self.write(text), output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(output.read_text(), text.replace(RESET, "SELECT pg_catalog.set_config('search_path', '\"fixture\"', false);\n", 1))
            invalid = self.root / (uuid.uuid4().hex + ".sql")
            self.assertNotEqual(invoke("section", self.write(text + "SELECT 1;\n"), invalid).returncode, 0)
            self.assertFalse(invalid.exists())

    def test_existing_or_failed_output_cannot_be_used(self):
        source = self.write(section("SELECT 1;\n"))
        output = self.root / "existing.sql"
        output.write_bytes(b"keep original")
        result = invoke("section", source, output)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(output.read_bytes(), b"keep original")
        missing = self.root / "missing" / "execution.sql"
        self.assertNotEqual(invoke("section", source, missing).returncode, 0)
        self.assertFalse(missing.exists())
        large_source = self.write(section("-- bounded synthetic body\n" * 4096))
        failed = self.root / "write-failure.sql"
        def limit_output():
            resource.setrlimit(resource.RLIMIT_FSIZE, (1024, 1024))
        result = invoke("section", large_source, failed, preexec_fn=limit_output)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(failed.exists())


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "requires disposable loopback PostgreSQL")
class PostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection = urlsplit(os.environ["TEST_DATABASE_URL"])
        database = connection.path.lstrip("/")
        if os.environ.get("GITHUB_ACTIONS") == "true":
            allowed = connection.hostname in ("localhost", "127.0.0.1") and (connection.port or 5432) == 5432 and database in ("ai_image_factory_test", "ai_image_factory_rehearsal")
        else:
            allowed = connection.hostname == "127.0.0.1" and connection.port == 56259 and database == "aif_submit_closure_test"
        if connection.scheme not in ("postgres", "postgresql") or not allowed:
            raise AssertionError("requires the dedicated CI database or owned local PostgreSQL fixture")
        cls.environment = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LC_ALL": "C",
            "PGHOST": connection.hostname, "PGPORT": str(connection.port or 5432),
            "PGUSER": connection.username or "", "PGPASSWORD": connection.password or "",
            "PGDATABASE": database, "PGCONNECT_TIMEOUT": "5"}
        cls.psql = os.environ.get("TEST_PSQL", "psql")
        migration = (ROOT / "crates/image-gateway/migrations/0143_dreamina_accepted_completion_budget.sql").read_text()
        cls.function = re.search(r"CREATE FUNCTION provider_task_completion_deadline\(.*?\n\$\$;", migration, re.S).group()
        start = migration.index("ALTER TABLE provider_remote_tasks\n    DROP CONSTRAINT provider_remote_tasks_poll_fence_check")
        cls.constraints = migration[start:migration.index("\n\n-- Patch the current definitions", start)]

    @classmethod
    def sql(cls, value=None, file=None):
        command = [cls.psql, "-X", "-qAt", "--set=ON_ERROR_STOP=1", "--set=VERBOSITY=verbose", "--single-transaction"]
        if file:
            command.append("--file=" + str(file))
        return subprocess.run(command, input=value, env=cls.environment, capture_output=True, text=True, timeout=30)

    def setUp(self):
        self.schema = "aif_rsp_" + uuid.uuid4().hex[:16]
        self.temporary = tempfile.TemporaryDirectory(prefix="aif-recovery-sql-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        result = self.sql(f'CREATE SCHEMA "{self.schema}"; CREATE TABLE "{self.schema}".sentinel(value TEXT); INSERT INTO "{self.schema}".sentinel VALUES (\'original\');')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.addCleanup(self.drop_schema)

    def drop_schema(self):
        result = self.sql(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE;')
        self.assertEqual(result.returncode, 0, result.stderr)

    def assert_original(self):
        result = self.sql(f'SELECT value FROM "{self.schema}".sentinel;')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "original")

    def fixture(self, with_budget=True, budget_first=True):
        schema = '"' + self.schema + '"'
        function = self.function.replace("CREATE FUNCTION provider_task_completion_deadline", "CREATE FUNCTION " + schema + ".provider_task_completion_deadline", 1)
        poll_check = re.search(r"ADD CONSTRAINT (provider_remote_tasks_poll_fence_check CHECK \(.*?\)),\n    DROP CONSTRAINT", self.constraints, re.S).group(1)
        deadline_check = re.search(r"ADD CONSTRAINT (provider_remote_tasks_deadline_shape_check CHECK \(.*\));$", self.constraints, re.S).group(1)
        checks = "CONSTRAINT " + poll_check + ",\nCONSTRAINT " + deadline_check
        checks = checks.replace("provider_task_completion_deadline(", schema + ".provider_task_completion_deadline(")
        pre = f'''CREATE SCHEMA {schema};
CREATE TABLE {schema}.accepted_completion_budgets(submission_id UUID PRIMARY KEY, original_submit_deadline_at_ms BIGINT NOT NULL, completion_deadline_at_ms BIGINT NOT NULL);
{function}
CREATE TABLE {schema}.provider_remote_tasks(submission_id UUID PRIMARY KEY, provider_deadline_at_ms BIGINT NOT NULL, created_at_ms BIGINT NOT NULL, state TEXT NOT NULL, next_poll_at_ms BIGINT, poll_owner TEXT, poll_lease_expires_at_ms BIGINT, poll_claimed_at_ms BIGINT, poll_lease_epoch BIGINT NOT NULL DEFAULT 0, deadline_quarantine_id UUID, artifact_ref TEXT, error_code TEXT, effect_certainty TEXT, terminal_at_ms BIGINT,
{checks});
CREATE TABLE {schema}.sentinel(value TEXT);
'''
        budget = f'COPY {schema}.accepted_completion_budgets(submission_id, original_submit_deadline_at_ms, completion_deadline_at_ms) FROM stdin;\n'
        if with_budget:
            budget += "00000000-0000-0000-0000-000000000001\t100\t1000\n"
        budget += "\\.\n"
        task = f'COPY {schema}.provider_remote_tasks(submission_id, provider_deadline_at_ms, created_at_ms, state, next_poll_at_ms) FROM stdin;\n00000000-0000-0000-0000-000000000001\t100\t1\tprovider_waiting\t500\n\\.\n'
        data = (budget + task if budget_first else task + budget) + f'COPY {schema}.sentinel(value) FROM stdin;\nrestored\n\\.\n'
        post = f'ALTER TABLE {schema}.sentinel ADD CONSTRAINT sentinel_value CHECK(value <> \'invalid\');\n'
        return [section(pre), section(data, data=True), section(post)]

    def execution_file(self, sections, normalize=False, tail=""):
        parts = []
        for index, text in enumerate(sections):
            source = self.root / f"section-{index}.sql"
            source.write_text(text)
            source.chmod(0o600)
            if normalize:
                output = self.root / f"normalized-{index}.sql"
                result = invoke("section", source, output, schema=self.schema)
                self.assertEqual(result.returncode, 0, result.stderr)
                parts.append(output.read_text())
            else:
                parts.append(text)
        execution = self.root / (uuid.uuid4().hex + ".sql")
        execution.write_text(f'DROP SCHEMA "{self.schema}" CASCADE;\n' + "".join(parts) + tail)
        execution.chmod(0o600)
        return execution

    def test_old_and_prepend_only_fail_then_complete_normalized_restore_succeeds(self):
        sections = self.fixture()
        self.assertLess(sections[1].index(".accepted_completion_budgets("), sections[1].index(".provider_remote_tasks("))
        for prepend in ("", f'SET search_path TO "{self.schema}";\n'):
            source = self.execution_file(sections)
            if prepend:
                source.write_text(prepend + source.read_text())
            result = self.sql(file=source)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("42P01", result.stderr)
            self.assertIn('relation "accepted_completion_budgets" does not exist', result.stderr)
            self.assertIn("provider_task_completion_deadline", result.stderr)
            self.assertIn("COPY provider_remote_tasks", result.stderr)
            self.assert_original()
        execution = self.execution_file(sections, normalize=True, tail="SELECT current_schema();\n")
        result = self.sql(file=execution)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines()[-1], self.schema)
        result = self.sql(f'SET search_path TO "{self.schema}"; SELECT provider_task_completion_deadline(\'00000000-0000-0000-0000-000000000001\',100), (SELECT next_poll_at_ms FROM provider_remote_tasks), (SELECT count(*) FROM accepted_completion_budgets);')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "1000|500|1")

    def test_no_budget_or_wrong_copy_order_keeps_constraints_and_rolls_back(self):
        for with_budget, budget_first in ((False, True), (True, False)):
            with self.subTest(with_budget=with_budget, budget_first=budget_first):
                sections = self.fixture(with_budget=with_budget, budget_first=budget_first)
                # Each attempt uses a separate private normalization directory.
                for path in self.root.glob("normalized-*.sql"):
                    path.unlink()
                execution = self.execution_file(sections, normalize=True)
                result = self.sql(file=execution)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("23514", result.stderr)
                self.assert_original()

    def test_generation_failure_cannot_publish_an_execution_file(self):
        source = self.root / "generation.sql"
        source.write_text(section(f'DROP SCHEMA "{self.schema}" CASCADE;\n' + "-- synthetic padding\n" * 4096))
        source.chmod(0o600)
        output = self.root / "failed-execution.sql"
        def limit_output():
            resource.setrlimit(resource.RLIMIT_FSIZE, (1024, 1024))
        result = invoke("section", source, output, schema=self.schema, preexec_fn=limit_output)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())
        result = self.sql(file=output)
        self.assertNotEqual(result.returncode, 0)
        self.assert_original()

    def test_later_sql_failure_cannot_commit_the_restored_prefix(self):
        execution = self.execution_file(self.fixture(), normalize=True,
            tail="DO $guard$ BEGIN RAISE EXCEPTION 'synthetic late failure'; END $guard$;\n")
        result = self.sql(file=execution)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("synthetic late failure", result.stderr)
        self.assert_original()
        result = self.sql(f"SELECT to_regclass('{self.schema}.accepted_completion_budgets') IS NULL;")
        self.assertEqual(result.stdout.strip(), "t")


if __name__ == "__main__":
    unittest.main()
