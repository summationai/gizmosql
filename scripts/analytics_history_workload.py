# Licensed under the Apache License, Version 2.0.
"""Recover one recurring date-range quality-check workload from query history."""

import hashlib
import json
import statistics
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

import sqlglot
from analytics_lineage import (
    allowed_args,
    quote_identifier,
    simple_column,
    simple_identifier,
)
from sqlglot import exp

HISTORY_BYTES = 8 * 1024 * 1024
READ_METHODS = {"DoGetStatement", "CreatePreparedStatement"}


def bound(node):
    if (
        type(node) is exp.Sub
        and type(node.this) is exp.CurrentDate
        and not any(node.this.args.values())
        and type(node.expression) is exp.Literal
        and node.expression.is_int
        and 0 <= int(node.expression.this) <= 3650
    ):
        return int(node.expression.this)
    if type(node) is exp.Literal and node.is_string:
        return date.fromisoformat(node.this)
    raise ValueError("unsupported date bound")


def count_argument(node):
    if type(node) is not exp.Count or not allowed_args(node, "this", "big_int"):
        return None
    if type(node.this) is exp.Star and not any(node.this.args.values()):
        return "*"
    if simple_column(node.this) and not node.this.table:
        return node.this.name.lower()
    return None


def quality_check(sql, relation):
    """Recognize COUNT(*) - COUNT(column), COUNT(*) over one date range."""
    try:
        parsed = sqlglot.parse(sql, read="duckdb")
        if len(parsed) != 1:
            return None
        query = parsed[0]
        if type(query) is not exp.Select or not allowed_args(
            query, "expressions", "from_", "where"
        ):
            return None
        source, where = query.args.get("from_"), query.args.get("where")
        if (
            type(source) is not exp.From
            or type(source.this) is not exp.Table
            or not where
        ):
            return None
        table = source.this
        if (
            not allowed_args(table, "catalog", "db", "this")
            or not all(map(simple_identifier, table.parts))
            or ".".join(p.name.lower() for p in table.parts) != relation
        ):
            return None
        if len(query.expressions) != 2:
            return None
        missing, total = query.expressions
        if not all(
            type(p) is exp.Alias and simple_identifier(p.args.get("alias"))
            for p in (missing, total)
        ):
            return None
        if (
            missing.alias.lower() == total.alias.lower()
            or type(missing.this) is not exp.Sub
            or count_argument(missing.this.this) != "*"
            or count_argument(total.this) != "*"
        ):
            return None
        measured = count_argument(missing.this.expression)
        if measured in (None, "*"):
            return None
        predicate = where.this.unnest()
        if type(predicate) is exp.Between and allowed_args(
            predicate, "this", "low", "high"
        ):
            low_col = high_col = predicate.this
            low, high = bound(predicate.args["low"]), bound(predicate.args["high"])
        elif type(predicate) is exp.And:
            comparisons = {
                type(p): p
                for p in (predicate.this.unnest(), predicate.expression.unnest())
            }
            if comparisons.keys() != {exp.GTE, exp.LTE}:
                return None
            low_term, high_term = comparisons[exp.GTE], comparisons[exp.LTE]
            low_col, high_col = low_term.this, high_term.this
            low, high = bound(low_term.expression), bound(high_term.expression)
        else:
            return None
        if (
            not all(simple_column(c) and not c.table for c in (low_col, high_col))
            or low_col.name.lower() != high_col.name.lower()
            or type(low) is not type(high)
        ):
            return None
        return (low_col.name.lower(), low, high), (measured, missing.alias, total.alias)
    except (sqlglot.errors.SqlglotError, RecursionError, ValueError):
        return None


def read_history(path, relation, days, columns):
    with path.open("rb") as handle:
        offset = max(0, handle.seek(0, 2) - HISTORY_BYTES)
        handle.seek(offset)
        events = []
        for raw in handle.read(HISTORY_BYTES).split(b"\n")[bool(offset) : -1]:
            try:
                event = json.loads(raw)
                if (
                    event.get("kind") != "sql"
                    or event.get("status") != "success"
                    or event.get("is_internal") not in (False, "false")
                    or event.get("flight_method") not in READ_METHODS
                ):
                    continue
                ts = datetime.fromisoformat(event["ts"])
                duration = float(event["duration_ms"])
                if ts.utcoffset() is None or not 0 <= duration < float("inf"):
                    continue
                events.append((ts, duration, event))
            except (AttributeError, KeyError, OverflowError, TypeError, ValueError):
                continue
    if not events:
        raise ValueError("no successful user SQL events in history")
    end = max(t for t, _, _ in events)
    groups = defaultdict(list)
    observed = 0
    for ts, duration, event in events:
        if ts <= end - timedelta(days=days):
            continue
        observed += 1
        sql = event.get("sql")
        if (
            isinstance(sql, str)
            and sql.lstrip().lower().startswith("select")
            and len(sql.encode()) <= 16 * 1024
        ):
            result = quality_check(sql, relation)
            if result and columns.get(result[0][0]) == "DATE" and result[1][0] in columns:
                key, check = result
                session = event.get("session_id")
                cohort = (
                    (
                        str(event.get("instance_id")),
                        session,
                        str(event.get("user")),
                        ts.date(),
                    )
                    if isinstance(session, str) and session
                    else None
                )
                groups[key].append((check, duration, cohort, event["flight_method"]))
    recurrent = {
        key: rows
        for key, rows in groups.items()
        if max(Counter(r[0] for r in rows).values()) >= 3
    }
    if not recurrent:
        raise ValueError("no supported quality checks observed at least three times")
    key, rows = max(
        recurrent.items(), key=lambda item: (sum(row[1] for row in item[1]), len(item[1]))
    )
    volumes = Counter(row[0] for row in rows)
    checks = sorted(
        (check for check in volumes if volumes[check] >= 3),
        key=lambda check: (-volumes[check], check),
    )[:8]
    checks.sort()
    by_check = defaultdict(list)
    by_cohort = defaultdict(Counter)
    methods = Counter()
    for check, duration, cohort, method in rows:
        if check in checks:
            by_check[check].append(duration)
            if cohort is not None:
                by_cohort[cohort][check] += 1
            methods[method] += 1
    batches = sum(min(counts[c] for c in checks) for counts in by_cohort.values())
    if len(checks) == 1:
        batches = volumes[checks[0]]
    evidence = {
        "window_days": days,
        "window_end": end.isoformat(),
        "read_events": observed,
        "supported_events": sum(map(len, groups.values())),
        "matching_events": sum(methods.values()),
        "method_counts": dict(methods),
        "cohort_read_sets": batches,
        "set_median_source_ms": sum(statistics.median(by_check[c]) for c in checks),
        "checks": [
            {
                "id": f"q{i}",
                "executions": volumes[c],
                "median_ms": statistics.median(by_check[c]),
                "p90_ms": statistics.quantiles(by_check[c], n=10, method="inclusive")[-1],
            }
            for i, c in enumerate(checks)
        ],
    }
    return key, checks, evidence


STRATEGIES = ("sorted-copy", "daily-rollup", "fused-checks")


def daily_build(day, counts, source):
    return f"SELECT {day}, COUNT(*) AS total, {', '.join(counts)} FROM {source} GROUP BY {day}"


def daily_queries(day, size, source, outputs):
    counts = [f"COALESCE(SUM(missing_{i}),0)::BIGINT AS missing_{i}" for i in range(size)]
    return [
        {
            "sql": f"SELECT COALESCE(SUM(total),0)::BIGINT AS total, {', '.join(counts)} "
            f"FROM {source} WHERE {day} BETWEEN ?::DATE AND ?::DATE",
            "bind": ["low", "high"],
            "outputs": outputs,
        }
    ]


def reusable_view(relation, date_column, checks, snapshot, candidate):
    """Describe the tested rollup as a reusable view contract for consumers."""
    if candidate["id"] != "daily-rollup" or candidate["status"] not in (
        "faster_on_replay",
        "equivalent",
    ):
        return None
    signature = json.dumps(
        [1, relation, date_column, checks], separators=(",", ":")
    ).encode()
    name = "quality_daily_" + hashlib.sha256(signature).hexdigest()[:16]
    table = name + "_data"
    day = quote_identifier(date_column)
    source = ".".join(map(quote_identifier, relation.split(".")))
    outputs, counts = quality_outputs(checks)
    return {
        "id": name,
        "kind": "view_over_rollup",
        "status": "suggested"
        if candidate["advice"]["status"] == "worth_followup"
        else "deferred",
        "source_relation": relation,
        "verified_on_export": snapshot,
        "grain": [date_column],
        "schema": {
            date_column: "DATE",
            "total": "BIGINT",
            **{f"missing_{i}": "BIGINT" for i in range(len(checks))},
        },
        "checks": [{"id": f"q{i}", "column": c[0]} for i, c in enumerate(checks)],
        "build_table_sql": f"CREATE TABLE {quote_identifier(table)} AS {daily_build(day, counts, source)}",
        "create_view_sql": f"CREATE VIEW {quote_identifier(name)} AS "
        f"SELECT * FROM {quote_identifier(table)}",
        "queries": daily_queries(day, len(checks), quote_identifier(name), outputs),
        "parameters": {"low": "DATE", "high": "DATE"},
        "requires": [
            "consumer_installation",
            "matching_access_scope",
            "fresh_backing_snapshot",
        ],
    }


def quality_outputs(checks):
    outputs, counts = {}, []
    for i, (column, missing, total) in enumerate(checks):
        outputs[f"q{i}"] = {missing: f"missing_{i}", total: "total"}
        counts.append(f"COUNT(*) - COUNT({quote_identifier(column)}) AS missing_{i}")
    return outputs, counts


def workload(snapshot, key, checks, anchors, strategies, repetitions):
    date_col, low, high = key
    day = quote_identifier(date_col)
    where = f"WHERE {day} BETWEEN ?::DATE AND ?::DATE"
    bind = ["low", "high"]
    if isinstance(low, int):
        if not anchors:
            raise ValueError(
                "relative date ranges require at least one --as-of archive date"
            )
        windows = [
            (anchor - timedelta(days=low), anchor - timedelta(days=high))
            for anchor in anchors
        ]
    else:
        windows = [(low, high)]
    if any(start > end for start, end in windows):
        raise ValueError("date range is empty")
    baseline = []
    projected, fused = quality_outputs(checks)
    for i, (column, missing, total) in enumerate(checks):
        name, count = f"q{i}", f"COUNT({quote_identifier(column)})"
        baseline.append(
            {
                "id": name,
                "bind": bind,
                "sql": f"SELECT COUNT(*) - {count} AS {quote_identifier(missing)}, "
                f"COUNT(*) AS {quote_identifier(total)} FROM data {where}",
            }
        )
    candidates = {
        "sorted-copy": {"sort_by": [date_col]},
        "daily-rollup": {
            "build_sql": daily_build(day, fused, "source"),
            "queries": daily_queries(day, len(checks), "data", projected),
        },
        "fused-checks": {
            "queries": [
                {
                    "sql": f"SELECT COUNT(*) AS total, {', '.join(fused)} FROM data {where}",
                    "bind": bind,
                    "outputs": projected,
                }
            ],
        },
    }
    return {
        "snapshot": snapshot,
        "baseline": baseline,
        "repetitions": repetitions,
        "cases": [
            {"id": f"window-{i}", "params": {"low": str(lo), "high": str(hi)}}
            for i, (lo, hi) in enumerate(windows)
        ],
        "candidates": [
            dict(candidates[strategy], id=strategy) for strategy in strategies
        ],
    }
