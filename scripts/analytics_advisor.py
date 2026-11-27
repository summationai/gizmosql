#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.
"""Use a local agent to propose and review offline analytics benchmarks."""

import argparse
import json
import math
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from analytics_daily_aggregate import STRATEGY as DAILY_SUM_STRATEGY
from analytics_daily_aggregate import daily_payoff, daily_workload, discover_daily_sums
from analytics_history_workload import (
    STRATEGIES,
    history_events,
    quality_history,
    reusable_view,
    workload,
)
from analytics_lineage import quote_identifier
from analytics_replay import replay, write_report

import duckdb


def ask_agent(model, instruction, evidence, choices, *, review=False):
    """Send bounded aggregate evidence to the model, with all tools disabled."""
    key = "decisions" if review else "proposals"
    properties = {
        "id": {"enum": list(choices)},
        "reason": {"type": "string", "maxLength": 600},
    }
    if review:
        properties["worth_followup"] = {"type": "boolean"}
    schema = {
        "type": "object",
        "required": [key],
        "additionalProperties": False,
        "properties": {
            key: {
                "type": "array",
                "minItems": 1,
                "maxItems": len(choices),
                "items": {
                    "type": "object",
                    "properties": properties,
                    "required": list(properties),
                    "additionalProperties": False,
                },
            }
        },
    }
    prompt = instruction + "\nEvidence (aggregate only):\n" + json.dumps(evidence)
    with TemporaryDirectory(prefix="gizmosql-advisor-model-") as isolated:
        process = subprocess.run(
            [
                "claude",
                "-p",
                "--model",
                model,
                "--effort",
                "medium",
                "--tools",
                "",
                "--strict-mcp-config",
                "--setting-sources",
                "",
                "--no-session-persistence",
                "--output-format",
                "json",
                "--json-schema",
                json.dumps(schema),
            ],
            input=prompt,
            text=True,
            capture_output=True,
            cwd=isolated,
            timeout=120,
        )
    if process.returncode:
        raise RuntimeError(f"proposal agent failed (exit {process.returncode})")
    try:
        items = json.loads(process.stdout)["structured_output"][key]
        ids = [item["id"] for item in items]
        if (
            len(set(ids)) != len(ids)
            or not set(ids).issubset(choices)
            or (review and set(ids) != set(choices))
            or any(
                not isinstance(item["reason"], str) or len(item["reason"]) > 600
                for item in items
            )
            or (
                review and any(type(item["worth_followup"]) is not bool for item in items)
            )
        ):
            raise ValueError("invalid structured response")
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise RuntimeError("agent returned an invalid structured response") from error
    return items


def table_churn(data, relation):
    if data is None:
        return {"basis": "unavailable", "commits_per_day": None}
    start, end = [datetime.fromisoformat(data[key]) for key in ("start", "end")]
    days = (end - start).total_seconds() / 86400
    if (
        data["relation"].lower() != relation
        or start.utcoffset() is None
        or end.utcoffset() is None
        or days <= 0
        or type(data["commits"]) is not int
        or data["commits"] < 0
        or data["basis"] not in ("observed", "archived_proxy")
    ):
        raise ValueError("invalid source table change evidence")
    return {
        "basis": data["basis"],
        "start": start.isoformat(),
        "end": end.isoformat(),
        "commits": data["commits"],
        "commits_per_day": data["commits"] / days,
    }


def estimate(candidate, history, churn, minimum):
    """Conservative triage estimate, in milliseconds of work per day."""
    stat = {"minimum_net_ms_per_day": minimum}
    if candidate["status"] != "faster_on_replay":
        return dict(stat, gate="replay_did_not_pass")
    artifact = candidate["strategy"] != "rewrite"
    rate = churn["commits_per_day"] if artifact else 0
    # Sorted data can serve independent calls. Batched rewrites and rollups
    # need a compatible cohort. This is observed demand, not an extrapolation
    # to unseen events or a guarantee that a caller will accept a batch.
    sets = (
        min(item["executions"] for item in history["checks"])
        if candidate["id"] == "sorted-copy"
        else history["cohort_read_sets"]
    )
    daily_sets = sets / history["window_days"]
    saved_fraction = min(
        c["median_saved_ms"] / c["baseline"]["median_ms"] for c in candidate["cases"]
    )
    benefit = daily_sets * history["set_median_source_ms"] * saved_fraction
    build = candidate["build_ms"] if artifact else 0
    refresh = None if rate is None else build * rate
    net = None if refresh is None else benefit - refresh - build / 30
    if benefit < minimum or (net is not None and net < minimum):
        gate = "insufficient_estimated_payoff"
    elif artifact and churn["basis"] != "observed":
        gate = "needs_current_source_churn"
    else:
        gate = "passes"
    return dict(
        stat,
        gate=gate,
        observed_sets_per_day=daily_sets,
        conservative_replay_saved_fraction=saved_fraction,
        estimated_saved_ms_per_day=benefit,
        estimated_refresh_ms_per_day=refresh,
        amortized_build_ms_per_day=build / 30,
        estimated_net_ms_per_day=net,
    )


PROPOSE = """You are the proposal stage for a local-only query optimization benchmark.
Choose and rank up to three of the supplied strategy IDs to replay. A strategy that might be
uneconomic is still useful as an offline comparison. Consider observed volume, source latency,
cohorts and commit rate. Treat sampled history as a lower bound on traffic and archived churn
as a proxy. Sort copies preserve independent queries; the daily rollup requires refresh and
the fused query requires a caller batch. Grouping by DATE before formatting is an independent
SQL rewrite with no artifact to refresh. Return decisions using only the provided schema."""

REVIEW = """You are the second stage of an offline query optimization benchmark.
Review each candidate using its verified replay, tail latency, observed workload latency and
volume, and rebuild/refresh estimate. Decide if further engineering is worth exploring.
A failing deterministic gate cannot be recommended. Archived change rates are proxies.
Consider the caller integration or refresh work even when replay is faster.
Return one decision per candidate ID using the provided schema."""


def advise(args, ask=ask_agent):
    if (
        not 0 < args.history_days <= 365
        or not math.isfinite(args.min_net_ms_per_day)
        or args.min_net_ms_per_day < 0
    ):
        raise ValueError("history days or payoff threshold outside allowed range")
    parquet = args.export.resolve(strict=True)
    if not parquet.is_file():
        raise ValueError("export must be a local file")
    relation = args.relation.lower()
    churn = table_churn(
        json.loads(args.changes.read_text()) if args.changes else None, relation
    )
    with duckdb.connect() as db:
        columns = dict(
            row[:2]
            for row in db.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet)]
            ).fetchall()
        )
        columns = {k.lower(): v for k, v in columns.items()}
        events, coverage = history_events(
            args.history,
            args.history_days,
            instrumentation=getattr(args, "instrumentation_history", None),
        )
        daily, history = discover_daily_sums(events, coverage, relation, columns)
        rewrite = None
        if daily:
            config, rewrite = daily_workload(args, relation, daily, db)
            feasible = [DAILY_SUM_STRATEGY]
        else:
            key, checks, history = quality_history(events, coverage, relation, columns)
            # Validate the experiment has data in each pinned date window.
            config = workload(
                args.snapshot, key, checks, args.as_of, [], args.repetitions
            )
            day = quote_identifier(key[0])
            for case in config["cases"]:
                result = db.execute(
                    f"SELECT count(*) FROM read_parquet(?) WHERE {day} BETWEEN ?::DATE AND ?::DATE",
                    [str(parquet), *case["params"].values()],
                ).fetchone()[0]
                if not result:
                    raise ValueError("replay date window contains no rows")
            feasible = list(
                STRATEGIES
                if history["cohort_read_sets"] and len(checks) > 1
                else STRATEGIES[:2]
                if len(checks) == 1
                else STRATEGIES[:1]
            )
        if churn["basis"] == "observed" and (
            datetime.fromisoformat(churn["end"])
            < datetime.fromisoformat(history["window_end"]) - timedelta(days=1)
        ):
            churn["basis"] = "archived_proxy"
    context = {
        "history": history,
        "source_changes": churn,
        "strategies": feasible,
        "minimum_net_ms_per_day": args.min_net_ms_per_day,
    }
    proposals = ask(args.model, PROPOSE, context, feasible)
    if not proposals:
        raise ValueError("agent selected no strategies for replay")
    if not daily:
        config = workload(
            args.snapshot,
            key,
            checks,
            args.as_of,
            [p["id"] for p in proposals],
            args.repetitions,
        )
    report = replay(config, parquet, include_workload=args.include_workload)
    candidates = report["candidates"]
    for candidate in candidates:
        candidate["payoff"] = (
            daily_payoff(candidate, history, args.min_net_ms_per_day)
            if daily
            else estimate(candidate, history, churn, args.min_net_ms_per_day)
        )
    reviews = [
        {
            "id": c["id"],
            "status": c["status"],
            "build_ms": c["build_ms"],
            "payoff": c["payoff"],
            "cases": [
                {k: case[k] for k in ("status", "baseline", "candidate") if k in case}
                for case in c["cases"]
            ],
        }
        for c in candidates
    ]
    try:
        decisions = {
            d["id"]: d
            for d in ask(
                args.model,
                REVIEW,
                dict(context, evaluations=reviews),
                [c["id"] for c in candidates],
                review=True,
            )
        }
    except (OSError, RuntimeError, subprocess.TimeoutExpired):
        # Preserve verified replay evidence when review is unavailable. An
        # unevaluated candidate must never be promoted by the payoff gate alone.
        decisions = {}
    for candidate, proposal in zip(candidates, proposals):
        decision = decisions.get(candidate["id"])
        outcome = candidate["payoff"]["gate"]
        candidate["proposal"] = proposal["reason"]
        candidate["advice"] = {
            "agent_reason": decision["reason"] if decision else None,
            "agent_worth_followup": decision["worth_followup"] if decision else None,
            "status": outcome
            if outcome != "passes"
            else (
                "agent_review_unavailable"
                if not decision
                else "worth_followup"
                if decision["worth_followup"]
                else "agent_deferred"
            ),
        }
        if daily:
            if candidate["status"] in ("equivalent", "faster_on_replay"):
                candidate["rewrite"] = rewrite
        else:
            view = reusable_view(relation, key[0], checks, args.snapshot, candidate)
            if view:
                candidate["view"] = view
    report["advisor"] = {
        "model": args.model,
        "relation": relation,
        "history": history,
        "source_changes": churn,
        "review": "complete" if decisions else "unavailable",
        "last_run_at": datetime.now(timezone.utc).isoformat(),
    }
    write_report(report, args.reports)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", required=True, type=Path, help="local Parquet export")
    parser.add_argument("--snapshot", required=True, help="export snapshot label")
    history = parser.add_mutually_exclusive_group(required=True)
    history.add_argument(
        "--instrumentation-history",
        type=Path,
        help="local GizmoSQL instrumentation Parquet export",
    )
    history.add_argument("--history", type=Path, help="legacy query history JSONL")
    parser.add_argument(
        "--relation", required=True, help="exact logical relation in history"
    )
    parser.add_argument(
        "--history-days",
        required=True,
        type=float,
        help="covered days in the query history sample",
    )
    parser.add_argument(
        "--changes", type=Path, help="optional commit count and observation window JSON"
    )
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        action="append",
        default=[],
        help="archive date anchor for relative checks; repeat for multiple windows",
    )
    parser.add_argument("--min-net-ms-per-day", type=float, default=1000)
    parser.add_argument("--repetitions", type=int, default=8)
    parser.add_argument("--model", default="claude-sonnet-5")
    parser.add_argument("--reports", required=True, type=Path)
    parser.add_argument("--include-workload", action="store_true")
    args = parser.parse_args()
    try:
        report = advise(args)
    except (
        duckdb.Error,
        OSError,
        KeyError,
        RuntimeError,
        TypeError,
        ValueError,
        subprocess.TimeoutExpired,
    ) as error:
        parser.exit(1, f"advisor failed: {error}\n")
    print(
        json.dumps(
            {
                "id": report["id"],
                "advice": {c["id"]: c["advice"]["status"] for c in report["candidates"]},
            }
        )
    )


if __name__ == "__main__":
    main()
