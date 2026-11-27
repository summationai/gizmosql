"""Local regression checks for the offline analytics advisor."""

import json
import sys
import unittest
from argparse import Namespace
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from analytics_advisor import advise, estimate, table_churn  # noqa: E402
from analytics_daily_aggregate import daily_sum, daily_workload  # noqa: E402
from analytics_history_workload import quality_check  # noqa: E402
from analytics_replay import write_report  # noqa: E402
from analytics_replay_api import latest_recommendations, report_at  # noqa: E402

RELATION = "main.checks"
TEMPLATE = (
    "SELECT COUNT(*) - COUNT({column}) AS missing, COUNT(*) AS total "
    "FROM main.checks WHERE day >= CURRENT_DATE - 9 AND day <= CURRENT_DATE - 2"
)

DAILY = (
    "SELECT STRFTIME(day, '%Y-%m-%d') AS day_label, SUM(amount) AS total "
    "FROM main.checks WHERE {filter} AND day BETWEEN "
    "CAST('2026-11-26' AS DATE) - INTERVAL '16' DAY AND "
    "CAST('2026-11-26' AS DATE) - INTERVAL '1' DAY GROUP BY 1 ORDER BY 1"
)


class AdvisorTest(unittest.TestCase):
    def test_match_rejects_unmodeled_sql(self):
        query = TEMPLATE.format(column="amount")
        self.assertEqual(
            quality_check(query, RELATION),
            (("day", 9, 2), ("amount", "missing", "total")),
        )
        for rejected in (
            query + " AND kind = 'excluded'",
            query + "; SELECT 1",
            query.replace("COUNT(amount)", "COUNT(DISTINCT amount)"),
            query.replace("main.checks", "other.checks"),
            query + " ORDER BY total",
        ):
            with self.subTest(query=rejected):
                self.assertIsNone(quality_check(rejected, RELATION))

    def test_payoff_gate_accounts_for_refresh_and_churn_quality(self):
        candidate = {
            "id": "daily-rollup",
            "strategy": "derived_table",
            "status": "faster_on_replay",
            "build_ms": 20,
            "cases": [{"median_saved_ms": 4, "baseline": {"median_ms": 8}}],
        }
        history = {"cohort_read_sets": 30, "window_days": 1, "set_median_source_ms": 8}
        churn = {"basis": "observed", "commits_per_day": 1}
        self.assertEqual(estimate(candidate, history, churn, 5)["gate"], "passes")
        noisy = dict(churn, commits_per_day=12)
        self.assertEqual(
            estimate(candidate, history, noisy, 5)["gate"],
            "insufficient_estimated_payoff",
        )
        proxy = dict(churn, basis="archived_proxy")
        self.assertEqual(
            estimate(candidate, history, proxy, 5)["gate"], "needs_current_source_churn"
        )
        self.assertEqual(
            estimate(candidate, history, table_churn(None, RELATION), 5)["gate"],
            "needs_current_source_churn",
        )

    def test_history_to_verified_report(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            export, history = root / "source.parquet", root / "queries.parquet"
            with duckdb.connect() as connection:
                connection.execute(
                    """
                    COPY (SELECT DATE '2026-01-02' AS day, NULL::INTEGER AS amount, 'x' AS kind
                          UNION ALL SELECT DATE '2026-01-02', 2, NULL) TO ?
                """,
                    [str(export)],
                )
            with duckdb.connect() as db:
                db.sql(
                    """
                                            SELECT row_number() OVER () AS execution_id,
                               TIMESTAMPTZ '2026-11-26 12:00:00+00' AS execution_start_time,
                               2 AS duration_ms, 'success' AS status, sql_text,
                               'CreatePreparedStatement' AS flight_method, false AS is_internal,
                               session_id, 'instance' AS instance_id, 'pilot' AS username,
                               false AS has_binds, materialization_hit
                        FROM (VALUES ('first'), ('second'), ('third')) AS sessions(session_id)
                        CROSS JOIN (VALUES (?), (?)) AS checks(sql_text)
                        CROSS JOIN (VALUES (false), (true)) AS origins(materialization_hit)
                                        """,
                    params=[
                        TEMPLATE.format(column="amount"),
                        TEMPLATE.format(column="kind"),
                    ],
                ).write_parquet(str(history))
            calls = []

            def agent(model, instruction, evidence, choices, *, review=False):
                calls.append(evidence)
                self.assertNotIn(RELATION, json.dumps(evidence))
                if review:
                    return [
                        dict(id=choice, reason="Test verdict", worth_followup=True)
                        for choice in choices
                    ]
                return [dict(id=choice, reason="Test proposal") for choice in choices]

            args = Namespace(
                export=export,
                history=None,
                instrumentation_history=history,
                relation=RELATION,
                snapshot="archive-1",
                changes=None,
                history_days=1,
                as_of=[date(2026, 1, 4)],
                min_net_ms_per_day=1000,
                repetitions=2,
                reports=root / "reports",
                model="test-agent",
                include_workload=False,
            )
            report = advise(args, ask=agent)
            self.assertEqual(len(calls), 2)
            self.assertEqual(report["advisor"]["history"]["cohort_read_sets"], 3)
            self.assertEqual(
                [check["executions"] for check in report["advisor"]["history"]["checks"]],
                [3, 3],
            )
            self.assertEqual(len(report["candidates"]), 3)
            self.assertNotIn("workload", report)
            for candidate in report["candidates"]:
                self.assertEqual(candidate["cases"][0]["status"], "equivalent")
                self.assertNotEqual(candidate["advice"]["status"], "worth_followup")
            self.assertIsNotNone(report_at(root / "reports", report["id"]))
            current = latest_recommendations(args.reports)
            self.assertEqual(current["run_id"], report["id"])
            self.assertEqual(current["last_run_at"], report["advisor"]["last_run_at"])
            rollup = next(
                c for c in current["recommendations"] if c["id"] == "daily-rollup"
            )
            view = rollup["view"]
            self.assertEqual(view["status"], "deferred")
            with duckdb.connect() as consumer:
                consumer.execute(
                    "CREATE TABLE main.checks AS FROM read_parquet(?)", [str(export)]
                )
                consumer.execute(view["build_table_sql"])
                consumer.execute(view["create_view_sql"])
                query = view["queries"][0]
                self.assertEqual(
                    consumer.execute(
                        query["sql"], ["2025-12-26", "2026-01-02"]
                    ).fetchone(),
                    (2, 1, 1),
                )
                self.assertEqual(
                    consumer.execute(
                        query["sql"], ["2026-02-01", "2026-02-08"]
                    ).fetchone(),
                    (0, 0, 0),
                )

            def failing_review(model, instruction, evidence, choices, *, review=False):
                if review:
                    raise RuntimeError("review agent unavailable")
                return [{"id": "fused-checks", "reason": "Test proposal"}]

            report = advise(args, ask=failing_review)
            self.assertEqual(report["advisor"]["review"], "unavailable")
            self.assertEqual(report["candidates"][0]["cases"][0]["status"], "equivalent")
            self.assertIsNone(report["candidates"][0]["advice"]["agent_worth_followup"])
            self.assertIsNotNone(report_at(root / "reports", report["id"]))
            latest = latest_recommendations(args.reports)
            self.assertEqual(latest["run_id"], report["id"])
            self.assertEqual(
                [c["id"] for c in latest["recommendations"]], ["fused-checks"]
            )

    def test_daily_sum_rejects_different_semantics(self):
        sql = DAILY.format(filter="TRUE")
        columns = {"day": "DATE", "amount": "DECIMAL(20,2)", "kind": "VARCHAR"}
        self.assertIsNotNone(daily_sum(sql, RELATION, columns))
        for invalid in (
            sql.replace("%Y-%m-%d", "%Y-%m"),
            sql.replace("ORDER BY 1", "ORDER BY 1 DESC"),
            sql.replace("SUM(amount)", "SUM(DISTINCT amount)"),
            sql.replace("WHERE TRUE", "WHERE amount > 0"),
            sql.replace("main.checks", "temp.checks"),
            sql + "; SELECT 1",
        ):
            with self.subTest(invalid=invalid):
                self.assertIsNone(daily_sum(invalid, RELATION, columns))
        self.assertIsNone(daily_sum(sql, RELATION, dict(columns, day="TIMESTAMP")))

    def test_daily_sum_rewrite_is_discovered_and_published_after_verification(self):
        with TemporaryDirectory() as directory, duckdb.connect() as db:
            root = Path(directory)
            export, history = root / "source.parquet", root / "queries.parquet"
            db.sql("""
                SELECT * FROM (VALUES
                    (DATE '2026-01-01', 1.25::DECIMAL(20,2), 'retail-east'),
                    (DATE '2026-01-01', 1.25::DECIMAL(20,2), 'retail-east'),
                    (DATE '2026-01-02', NULL::DECIMAL(20,2), 'retail-east'),
                    (DATE '2026-01-02', 3.00::DECIMAL(20,2), 'retail-west'),
                    (NULL, 99.00::DECIMAL(20,2), 'retail-east')
                ) AS sample(day, amount, kind)
            """).write_parquet(str(export))
            originals = [DAILY.format(filter=f) for f in ("TRUE", "kind = 'retail-east'")]
            db.sql(
                """
                SELECT row_number() OVER () AS execution_id, sql_text,
                       TIMESTAMPTZ '2026-11-27 12:00:00+00' AS execution_start_time,
                       250 AS duration_ms, 'success' AS status,
                       'CreatePreparedStatement' AS flight_method, false AS is_internal,
                       'session' AS session_id, 'instance' AS instance_id, 'test' AS username,
                       false AS materialization_hit, false AS has_binds
                FROM (VALUES (?), (?)) AS queries(sql_text), range(3)
            """,
                params=originals,
            ).write_parquet(str(history))
            args = Namespace(
                export=export,
                history=None,
                instrumentation_history=history,
                relation=RELATION,
                snapshot="archive-1",
                changes=None,
                history_days=1,
                as_of=[date(2026, 1, 4)],
                min_net_ms_per_day=0,
                repetitions=2,
                reports=root / "reports",
                model="test-agent",
                include_workload=False,
            )

            def agent(model, instruction, evidence, choices, *, review=False):
                self.assertNotIn(RELATION, json.dumps(evidence))
                self.assertNotIn("retail-east", json.dumps(evidence))
                self.assertEqual(choices, ["group-by-date"])
                decision = {"id": choices[0], "reason": "Test review"}
                return [dict(decision, worth_followup=True) if review else decision]

            report = advise(args, ask=agent)
            self.assertEqual(report["advisor"]["history"]["matching_events"], 6)
            (rec,) = latest_recommendations(args.reports)["recommendations"]
            self.assertEqual(rec["id"], "group-by-date")
            rewrite = rec["rewrite"]
            self.assertEqual(rewrite["verified_on_export"], "archive-1")
            self.assertNotIn("retail-east", json.dumps(rewrite))
            db.execute("CREATE TABLE main.checks AS FROM read_parquet(?)", [str(export)])
            for example in rewrite["queries"]:
                filtered = "filter" in example["parameters"]
                bound = {"anchor": "2026-01-04", "filter": "retail-east"}
                actual = db.execute(
                    example["sql"], [bound[key] for key in example["bind"]]
                ).fetchall()
                expected = db.execute(
                    originals[filtered].replace("2026-11-26", "2026-01-04")
                ).fetchall()
                self.assertEqual(actual, expected)

            def wrong_workload(*params):
                config, contract = daily_workload(*params)
                query = config["candidates"][0]["queries"][0]
                query["sql"] = query["sql"].replace("total AS", "0::DECIMAL(38,2) AS")
                return config, contract

            with patch("analytics_advisor.daily_workload", side_effect=wrong_workload):
                rejected = advise(args, ask=agent)["candidates"][0]
            self.assertEqual(rejected["status"], "rejected")
            self.assertNotIn(
                "rewrite", latest_recommendations(args.reports)["recommendations"][0]
            )

    def test_latest_uses_advisor_completion_time(self):
        with TemporaryDirectory() as directory:
            reports = Path(directory)
            self.assertIsNone(latest_recommendations(reports)["last_run_at"])
            base = {"format": 1, "dataset": {}, "candidates": []}
            older = dict(
                base,
                id="a" * 32,
                created_at="2026-11-27T12:00:00+00:00",
                advisor={"last_run_at": "2026-11-27T12:05:00+00:00"},
            )
            newer = dict(
                base,
                id="b" * 32,
                created_at="2026-11-27T11:58:00+00:00",
                advisor={"last_run_at": "2026-11-27T12:10:00+00:00"},
            )
            unrelated = dict(base, id="c" * 32, created_at="2026-11-27T13:00:00+00:00")
            for report in (newer, older, unrelated):
                write_report(report, reports)
            latest = latest_recommendations(reports)
            self.assertEqual(latest["run_id"], newer["id"])
            self.assertEqual(latest["last_run_at"], newer["advisor"]["last_run_at"])


if __name__ == "__main__":
    unittest.main()
