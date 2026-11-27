"""Local regression checks for the offline analytics advisor."""

from argparse import Namespace
from datetime import date
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from analytics_advisor import advise, estimate, table_churn  # noqa: E402
from analytics_history_workload import quality_check  # noqa: E402
from analytics_replay import write_report  # noqa: E402
from analytics_replay_api import latest_recommendations, report_at  # noqa: E402


RELATION = "main.checks"
TEMPLATE = ("SELECT COUNT(*) - COUNT({column}) AS missing, COUNT(*) AS total "
            "FROM main.checks WHERE day >= CURRENT_DATE - 9 AND day <= CURRENT_DATE - 2")


class AdvisorTest(unittest.TestCase):
    def test_match_rejects_unmodeled_sql(self):
        query = TEMPLATE.format(column="amount")
        self.assertEqual(quality_check(query, RELATION), (("day", 9, 2), ("amount", "missing", "total")))
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
            "id": "daily-rollup", "strategy": "derived_table", "status": "faster_on_replay",
            "build_ms": 20, "cases": [{"median_saved_ms": 4, "baseline": {"median_ms": 8}}],
        }
        history = {"cohort_read_sets": 30, "window_days": 1, "set_median_source_ms": 8}
        churn = {"basis": "observed", "commits_per_day": 1}
        self.assertEqual(estimate(candidate, history, churn, 5)["gate"], "passes")
        noisy = dict(churn, commits_per_day=12)
        self.assertEqual(estimate(candidate, history, noisy, 5)["gate"], "insufficient_estimated_payoff")
        proxy = dict(churn, basis="archived_proxy")
        self.assertEqual(estimate(candidate, history, proxy, 5)["gate"], "needs_current_source_churn")
        self.assertEqual(estimate(candidate, history, table_churn(None, RELATION), 5)["gate"],
                         "needs_current_source_churn")

    def test_history_to_verified_report(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            export, history = root / "source.parquet", root / "queries.jsonl"
            with duckdb.connect() as connection:
                connection.execute("""
                    COPY (SELECT DATE '2026-01-02' AS day, NULL::INTEGER AS amount, 'x' AS kind
                          UNION ALL SELECT DATE '2026-01-02', 2, NULL) TO ?
                """, [str(export)])
            with history.open("w") as log:
                for session in ("first", "second", "third"):
                    for column in ("amount", "kind"):
                        log.write(json.dumps({
                            "ts": "2026-11-26T12:00:00Z", "duration_ms": 2,
                            "kind": "sql", "is_internal": False, "status": "success",
                            "flight_method": "CreatePreparedStatement", "session_id": session,
                            "sql": TEMPLATE.format(column=column),
                        }) + "\n")
            calls = []

            def agent(model, instruction, evidence, choices, *, review=False):
                calls.append(evidence)
                self.assertNotIn(RELATION, json.dumps(evidence))
                if review:
                    return [dict(id=choice, reason="Test verdict", worth_followup=True) for choice in choices]
                return [dict(id=choice, reason="Test proposal") for choice in choices]

            args = Namespace(
                export=export, history=history, relation=RELATION, snapshot="archive-1",
                changes=None, history_days=1, as_of=[date(2026, 1, 4)], min_net_ms_per_day=1000,
                repetitions=2, reports=root / "reports", model="test-agent", include_workload=False,
            )
            report = advise(args, ask=agent)
            self.assertEqual(len(calls), 2)
            self.assertEqual(report["advisor"]["history"]["cohort_read_sets"], 3)
            self.assertEqual(len(report["candidates"]), 3)
            self.assertNotIn("workload", report)
            for candidate in report["candidates"]:
                self.assertEqual(candidate["cases"][0]["status"], "equivalent")
                self.assertNotEqual(candidate["advice"]["status"], "worth_followup")
            self.assertIsNotNone(report_at(root / "reports", report["id"]))
            current = latest_recommendations(args.reports)
            self.assertEqual(current["run_id"], report["id"])
            self.assertEqual(current["last_run_at"], report["advisor"]["last_run_at"])
            rollup = next(c for c in current["recommendations"] if c["id"] == "daily-rollup")
            view = rollup["view"]
            self.assertEqual(view["status"], "deferred")
            with duckdb.connect() as consumer:
                consumer.execute("CREATE TABLE main.checks AS FROM read_parquet(?)", [str(export)])
                consumer.execute(view["build_table_sql"])
                consumer.execute(view["create_view_sql"])
                query = view["queries"][0]
                self.assertEqual(consumer.execute(query["sql"], ["2025-12-26", "2026-01-02"]).fetchone(),
                                 (2, 1, 1))
                self.assertEqual(consumer.execute(query["sql"], ["2026-02-01", "2026-02-08"]).fetchone(),
                                 (0, 0, 0))

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
            self.assertEqual([c["id"] for c in latest["recommendations"]], ["fused-checks"])

    def test_latest_uses_advisor_completion_time(self):
        with TemporaryDirectory() as directory:
            reports = Path(directory)
            self.assertIsNone(latest_recommendations(reports)["last_run_at"])
            base = {"format": 1, "dataset": {}, "candidates": []}
            older = dict(base, id="a" * 32, created_at="2026-11-27T12:00:00+00:00",
                         advisor={"last_run_at": "2026-11-27T12:05:00+00:00"})
            newer = dict(base, id="b" * 32, created_at="2026-11-27T11:58:00+00:00",
                         advisor={"last_run_at": "2026-11-27T12:10:00+00:00"})
            unrelated = dict(base, id="c" * 32, created_at="2026-11-27T13:00:00+00:00")
            for report in (newer, older, unrelated):
                write_report(report, reports)
            latest = latest_recommendations(reports)
            self.assertEqual(latest["run_id"], newer["id"])
            self.assertEqual(latest["last_run_at"], newer["advisor"]["last_run_at"])


if __name__ == "__main__":
    unittest.main()
