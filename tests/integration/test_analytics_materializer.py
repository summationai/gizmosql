"""Run with Python unittest. The Flight SQL case uses isolated local binaries."""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from analytics_lineage import Edge, candidate, changes_affect  # noqa: E402
from analytics_materializer import recent_queries  # noqa: E402

SOURCE = ("lake", "main", "orders")
QUERY = "SELECT SUM(total) AS gross FROM lake.main.orders WHERE region = 'EU'"
VIEW_QUERY = "SELECT SUM(total) AS gross FROM lake.main.pilot_orders"
INVENTORY_QUERY = "SELECT COUNT(*) AS stocked FROM lake.main.inventory"


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class EligibilityTest(unittest.TestCase):
    def test_approved_aggregates_only(self):
        for query in (
            QUERY,
            "select count(*) as n from lake.main.orders",
            "select region, count(*) as n from lake.main.orders group by region",
        ):
            with self.subTest(query=query):
                self.assertEqual(candidate(query, {SOURCE})[1], (SOURCE,))
        for query in (
            QUERY.replace("lake.main.orders", "lake.main.private_orders"),
            QUERY.replace("lake.main.orders", "read_parquet('/data/file')"),
            QUERY + "; DROP TABLE lake.main.orders",
            QUERY + " ORDER BY gross LIMIT 1",
            QUERY.replace("SUM(total)", "SUM(DISTINCT total)"),
            QUERY.replace("SUM(total)", "SUM(total + random())"),
            QUERY.replace("'EU'", "?"),
            QUERY.replace("'EU'", "(SELECT 'EU')"),
            QUERY.replace(
                "SUM(total) AS gross", "SUM(total) AS gross, COUNT(*) AS gross"
            ),
            "SELECT COUNT(*) AS n FROM (SELECT * FROM lake.main.orders) AS other",
            "SELECT COUNT(*) AS n FROM lake.main.orders JOIN lake.main.private_orders USING (region)",
        ):
            with self.subTest(query=query):
                self.assertIsNone(candidate(query, {SOURCE}))

    def test_changeset_requires_contiguous_known_history(self):
        anchor, head = (4, 100, 1), (5, 200, 1)
        edges = {Edge("mv", "lake.main.orders", "table", 17, "uuid")}
        first = (*anchor, {"tables_created": ["main.orders"]})
        self.assertFalse(
            changes_affect(
                [first, (*head, {"inlined_insert": ["18"]})], anchor, head, edges
            )
        )
        for history in (
            [(*head, {"tables_inserted_into": ["18"]})],
            [first, (*head, {"tables_deleted_from": ["17"]})],
            [first, (*head, {"tables_altered": ["17"]})],
            [first, (*head, {"new_change_kind": ["18"]})],
            [first, (*head, {"inlined_delete": ["invalid"]})],
            [first, (*head, {})],
            [(4, 99, 1, first[-1]), (*head, {"inlined_insert": ["18"]})],
        ):
            with self.subTest(history=history):
                self.assertTrue(changes_affect(history, anchor, head, edges))

    def test_history_filters_and_ignores_partial_log_record(self):
        now = datetime.now(UTC)
        good = dict(
            sql=QUERY,
            kind="sql",
            status="success",
            is_internal="false",
            user="pilot",
            ts=now.isoformat(),
            direct_execution="false",
            flight_method="DoGetStatement",
            duration_ms=20,
        )
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "query.jsonl"
            events = [
                good,
                {**good, "user": "other"},
                {**good, "status": "failure"},
                {**good, "flight_method": "GetFlightInfoStatement"},
                dict(
                    kind="analytics",
                    status="hit",
                    sql=QUERY,
                    user="pilot",
                    ts=now.isoformat(),
                ),
                {**good, "ts": (now - timedelta(hours=2)).isoformat()},
            ]
            log.write_text(
                "bad json\n"
                + "\n".join(map(json.dumps, events))
                + "\n"
                + json.dumps(good)
            )
            count, duration, hits = recent_queries(log, "pilot", timedelta(hours=1), now)
            self.assertEqual(count, {QUERY: 1})
            self.assertEqual(duration, {QUERY: 20})
            self.assertEqual(hits, {QUERY: 1})


@unittest.skipUnless(
    all(
        os.environ.get(name)
        for name in (
            "GIZMOSQL_TEST_BIN_DIR",
            "GIZMOSQL_TEST_POSTGRES_BIN_DIR",
            "GIZMOSQL_TEST_EXTENSION_DIR",
        )
    ),
    "set GizmoSQL, PostgreSQL and DuckDB extension directories for the Flight SQL integration",
)
class FlightSQLMaterializationTest(unittest.TestCase):
    def test_real_history_and_external_commit(self):
        with tempfile.TemporaryDirectory(prefix="gizmosql-mv-") as directory:
            root = Path(directory)
            pg_bin = Path(os.environ["GIZMOSQL_TEST_POSTGRES_BIN_DIR"])
            gizmo_bin = Path(os.environ["GIZMOSQL_TEST_BIN_DIR"])
            extension_dir = Path(os.environ["GIZMOSQL_TEST_EXTENSION_DIR"])
            pg_port = unused_port()
            pg_data = root / "pg"
            self.command(
                pg_bin / "initdb",
                "-D",
                pg_data,
                "-U",
                "postgres",
                "--auth=trust",
                "--no-locale",
            )
            self.command(
                pg_bin / "pg_ctl",
                "-D",
                pg_data,
                "-l",
                root / "pg.log",
                "-o",
                f"-h 127.0.0.1 -p {pg_port} -k {root}",
                "-w",
                "start",
            )
            try:
                self.exercise_flow(root, pg_port, unused_port(), gizmo_bin, extension_dir)
            finally:
                self.command(pg_bin / "pg_ctl", "-D", pg_data, "-m", "fast", "-w", "stop")

    def exercise_flow(self, root, pg_port, flight_port, gizmo_bin, extension_dir):
        load = "\n".join(
            f"LOAD '{extension_dir / (ext + '.duckdb_extension')}';"
            for ext in ("httpfs", "postgres_scanner", "ducklake")
        )
        attach = (
            f"{load}\nATTACH 'ducklake:postgres:host=127.0.0.1 port={pg_port} "
            f"dbname=postgres user=postgres' AS lake (DATA_PATH '{root / 'lake'}'"
        )
        init = root / "server.sql"
        worker_init = root / "worker.sql"
        init.write_text(attach + ");")
        worker_init.write_text(attach + ", READ_ONLY);")
        with duckdb.connect() as writer:
            writer.execute(init.read_text())
            writer.execute("CREATE TABLE lake.main.orders (region VARCHAR, total BIGINT)")
            writer.execute("INSERT INTO lake.main.orders VALUES ('EU', 11), ('US', 99)")
            writer.execute("CREATE SCHEMA lake.audit")
            writer.execute(
                "CREATE TABLE lake.audit.orders (region VARCHAR, total BIGINT)"
            )
            writer.execute("CREATE TABLE lake.main.inventory AS SELECT 1 AS id")
            writer.execute(
                "CREATE TABLE lake.main.regions AS SELECT 'EU' AS region UNION ALL SELECT 'US'"
            )
            writer.execute("""
                CREATE VIEW lake.main.filtered_orders AS
                SELECT region, total FROM lake.main.orders WHERE region = 'EU'
            """)
            writer.execute("""
                CREATE VIEW lake.main.pilot_orders AS
                SELECT o.total FROM lake.main.filtered_orders o
                JOIN lake.main.regions r ON o.region = r.region
            """)
        query_log = root / "query.jsonl"
        state = root / "state"
        exercise_execution = not os.environ.get("GIZMOSQL_TEST_BUILD_ONLY")
        server_init = init
        if exercise_execution:
            server_init = root / "sealed-server.sql"
            server_init.write_text(
                worker_init.read_text()
                + """
                SET enable_external_access = false;
                SET allowed_configs = ['default_collation'];
                SET lock_configuration = true;
            """
            )
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("GIZMOSQL_")
        }
        env["GIZMOSQL_PASSWORD"] = "materializer-integration-only"
        if exercise_execution:
            config = root / "materialization.json"
            config.write_text(
                json.dumps(
                    dict(
                        state_dir=str(state),
                        catalog="lake",
                        source_epoch="integration",
                        user="pilot",
                    )
                )
            )
            env["GIZMOSQL_ANALYTICS_CACHE_CONFIG"] = str(config)
        startup_log = root / "startup.log"
        with startup_log.open("w") as startup:
            server = subprocess.Popen(
                [
                    gizmo_bin / "gizmosql_server",
                    "--hostname",
                    "127.0.0.1",
                    "--port",
                    str(flight_port),
                    "--health-port",
                    "0",
                    "--username",
                    "pilot",
                    "--init-sql-commands-file",
                    server_init,
                    "--print-queries",
                    "--log-format",
                    "json",
                    "--query-log-level",
                    "info",
                    "--log-file",
                    query_log,
                ],
                env=env,
                stdout=startup,
                stderr=subprocess.STDOUT,
            )
        try:
            client = (
                gizmo_bin / "gizmosql_client",
                "--host",
                "127.0.0.1",
                "--port",
                flight_port,
                "--username",
                "pilot",
                "--no-init",
                "--quiet",
                "--csv",
                "--command",
            )
            deadline = time.monotonic() + 35
            while time.monotonic() < deadline and server.poll() is None:
                try:
                    with socket.create_connection(
                        ("127.0.0.1", flight_port), timeout=0.2
                    ):
                        break
                except OSError:
                    time.sleep(0.12)
            else:
                details = (
                    query_log.read_text()
                    if query_log.exists()
                    else startup_log.read_text()
                )
                self.fail(details[-3000:] or "server did not start")

            worker = (
                sys.executable,
                REPO / "scripts/analytics_materializer.py",
                "--query-log",
                query_log,
                "--source-init-sql",
                worker_init,
                "--source-table",
                ".".join(SOURCE),
                "--source-table",
                "lake.main.regions",
                "--source-table",
                "lake.main.inventory",
                "--source-view",
                "lake.main.filtered_orders",
                "--source-view",
                "lake.main.pilot_orders",
                "--source-epoch",
                "integration",
                "--state-dir",
                state,
                "--user",
                "pilot",
                "--min-runs",
                2,
                "--min-duration-ms",
                0,
                "--once",
            )
            if exercise_execution:
                worker += ("--publish",)

            def artifact_hits():
                return query_log.read_text().count("Analytics materialization hit")

            def flight_query(expected, *, hit):
                before = artifact_hits()
                self.assertIn(
                    str(expected), self.command(*client, VIEW_QUERY, env=env).splitlines()
                )
                self.assertEqual(
                    artifact_hits() - before, int(hit), query_log.read_text()[-4000:]
                )

            def artifacts():
                with duckdb.connect(
                    str(state / "artifacts.duckdb"), read_only=True
                ) as store:
                    rows = store.execute("""
                        SELECT name, source_table, snapshot_id, epoch_us(refreshed_at)
                        FROM materializations
                    """).fetchall()
                    return {
                        source: (
                            name,
                            snapshot,
                            refreshed,
                            store.execute(f'SELECT * FROM "{name}"').fetchone()[0],
                        )
                        for name, source, snapshot, refreshed in rows
                    }

            def checked_snapshots():
                with duckdb.connect(
                    str(state / "artifacts.duckdb"), read_only=True
                ) as store:
                    return dict(
                        store.execute(
                            "SELECT name, snapshot_id FROM materialization_refresh_state"
                        ).fetchall()
                    )

            self.assertIn("11", self.command(*client, VIEW_QUERY, env=env).splitlines())
            self.command(*client, INVENTORY_QUERY, env=env)
            self.command(*worker)
            self.assertEqual(artifacts(), {})
            self.command(*client, VIEW_QUERY, env=env)
            self.command(*client, INVENTORY_QUERY, env=env)
            self.command(*worker)
            first = artifacts()
            orders, inventory = "lake.main.pilot_orders", "lake.main.inventory"
            self.assertEqual((first[orders][-1], first[inventory][-1]), (11, 1))
            if exercise_execution:
                flight_query(11, hit=True)
                before = artifact_hits()
                self.assertIn(
                    "1", self.command(*client, INVENTORY_QUERY, env=env).splitlines()
                )
                self.assertEqual(artifact_hits() - before, 1)
                # A log rollover can leave only cache hits in the history window.
                hit_events = [
                    line
                    for line in query_log.read_text().splitlines()
                    if json.loads(line).get("status") == "hit"
                ]
                after_rollover = root / "rollover.jsonl"
                after_rollover.write_text("\n".join(hit_events) + "\n")
                self.command(*worker, "--query-log", after_rollover)
                self.assertEqual(artifacts(), first)
                (blob,) = (state / "published").glob(f"{first[orders][0]}_*.arrow")
                hidden = blob.with_suffix(".held")
                blob.rename(hidden)
                try:
                    flight_query(11, hit=False)
                finally:
                    hidden.rename(blob)
                before = artifact_hits()
                self.assertIn(
                    "11",
                    self.command(
                        *client, f"BEGIN; {VIEW_QUERY}; ROLLBACK;", env=env
                    ).splitlines(),
                )
                self.assertEqual(artifact_hits(), before)
                shadowed = f"CREATE TEMP MACRO sum(x) AS CAST(COUNT(x) AS HUGEINT); {VIEW_QUERY};"
                self.assertIn("1", self.command(*client, shadowed, env=env).splitlines())
                self.assertEqual(artifact_hits(), before)
                changed_settings = (
                    f"SET SESSION default_collation = 'nocase'; {VIEW_QUERY};"
                )
                self.assertIn(
                    "11", self.command(*client, changed_settings, env=env).splitlines()
                )
                self.assertEqual(artifact_hits(), before)
                path_reads = self.run_command(
                    *client,
                    f"SELECT content FROM read_blob('{state / 'identity.key'}')",
                    env=env,
                )
                self.assertIn("disabled by configuration", path_reads.stdout)
            with duckdb.connect(str(state / "artifacts.duckdb"), read_only=True) as store:
                # Follow base-table edges in the downstream direction as Modeler does.
                paths = store.execute("""
                    WITH RECURSIVE downstream(relation, depth) AS (
                        SELECT 'lake.main.orders', 0
                        UNION ALL
                        SELECT e.dependent, d.depth + 1
                        FROM downstream d JOIN materialization_dependencies e ON e.source_relation = d.relation
                    )
                    SELECT relation, depth FROM downstream ORDER BY depth
                """).fetchall()
                self.assertEqual(
                    paths,
                    [
                        ("lake.main.orders", 0),
                        ("lake.main.filtered_orders", 1),
                        (orders, 2),
                        (first[orders][0], 3),
                    ],
                )
            self.command(*worker)
            self.assertEqual(artifacts(), first)

            with duckdb.connect() as writer:
                writer.execute(init.read_text())
                # Same unqualified table name, different schema and stable DuckLake object ID.
                writer.execute("INSERT INTO lake.audit.orders VALUES ('EU', 500)")
                if exercise_execution:
                    flight_query(11, hit=False)
                self.assertNotIn("published", self.command(*worker))
                self.assertEqual(artifacts(), first)
                for name, snapshot, _, _ in first.values():
                    self.assertGreater(checked_snapshots()[name], snapshot)
                if exercise_execution:
                    flight_query(11, hit=True)

                writer.execute("ALTER TABLE lake.audit.orders ADD COLUMN note VARCHAR")
                self.assertNotIn("published", self.command(*worker))
                self.assertEqual(artifacts(), first)

                writer.execute("INSERT INTO lake.main.orders VALUES ('EU', 5)")
                if exercise_execution:
                    flight_query(
                        16, hit=False
                    )  # the published artifact still contains 11
                    self.assertEqual(artifacts(), first)
                self.assertEqual(self.command(*worker).count("published"), 1)
                second = artifacts()
                self.assertEqual(second[inventory], first[inventory])
                self.assertEqual(second[orders][0], first[orders][0])
                self.assertGreater(second[orders][1], first[orders][1])
                self.assertEqual(second[orders][-1], 16)
                flight_query(16, hit=exercise_execution)

                writer.execute("INSERT INTO lake.main.regions VALUES ('EU')")
                self.assertEqual(self.command(*worker).count("published"), 1)
                third = artifacts()
                self.assertEqual(third[orders][-1], 32)
                self.assertEqual(third[inventory], second[inventory])

                writer.execute("INSERT INTO lake.main.inventory VALUES (2)")
                self.assertEqual(self.command(*worker).count("published"), 1)
                fourth = artifacts()
                self.assertEqual(fourth[orders], third[orders])
                self.assertEqual(fourth[inventory][-1], 2)

                writer.begin()
                writer.execute("DELETE FROM lake.main.orders WHERE region = 'EU'")
                writer.rollback()
                self.assertNotIn("published", self.command(*worker))
                self.assertEqual(artifacts(), fourth)

                writer.execute("""
                    CREATE OR REPLACE VIEW lake.main.filtered_orders AS
                    SELECT region, total FROM lake.main.orders WHERE region = 'US'
                """)
                self.assertEqual(self.command(*worker).count("published"), 1)
                fifth = artifacts()
                self.assertEqual(fifth[orders][-1], 99)
                self.assertEqual(fifth[inventory], fourth[inventory])

                writer.execute("ALTER TABLE lake.main.orders DROP COLUMN total")
                result = self.run_command(*worker)
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn("refresh failed", result.stdout)
                self.assertEqual(artifacts(), fifth)
                if exercise_execution:
                    self.assertFalse(
                        list((state / "published").glob(f"{first[orders][0]}_*.arrow"))
                    )
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)

    def run_command(self, *args, env=None):
        return subprocess.run(
            [str(arg) for arg in args],
            env=env,
            timeout=30,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

    def command(self, *args, env=None):
        result = self.run_command(*args, env=env)
        self.assertEqual(result.returncode, 0, result.stdout)
        return result.stdout


if __name__ == "__main__":
    unittest.main()
