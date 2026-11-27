# Licensed under the Apache License, Version 2.0.
"""Discover and replay daily SUM queries that format their grouping key."""

import statistics
from collections import Counter, defaultdict
from datetime import date
from typing import NamedTuple

import sqlglot
from analytics_history import QUERY_BYTES
from analytics_lineage import (
    allowed_args,
    quote_identifier,
    simple_column,
    simple_identifier,
)
from sqlglot import exp

STRATEGY = "group-by-date"
DATE_FORMAT = "%Y-%m-%d"


class DailySum(NamedTuple):
    day: str
    measure: str
    day_label: str
    sum_label: str
    filter_column: str
    first_days_ago: int
    last_days_ago: int


def date_offset(node):
    if type(node) is not exp.Sub or not allowed_args(node, "this", "expression"):
        raise ValueError("unsupported date offset")
    anchor, offset = node.this, node.expression
    if (
        type(anchor) is not exp.Cast
        or not allowed_args(anchor, "this", "to")
        or anchor.to.this != exp.DataType.Type.DATE
        or type(anchor.this) is not exp.Literal
        or not anchor.this.is_string
        or type(offset) is not exp.Interval
        or not allowed_args(offset, "this", "unit")
        or type(offset.args.get("unit")) is not exp.Var
        or offset.args["unit"].name.upper() != "DAY"
        or type(offset.this) is not exp.Literal
        or not offset.this.name.isascii()
        or not offset.this.name.isdecimal()
    ):
        raise ValueError("unsupported date offset")
    days = int(offset.this.name)
    if days > 3650:
        raise ValueError("unsupported date offset")
    return date.fromisoformat(anchor.this.name), days


def column(node):
    return node.name.lower() if simple_column(node) and not node.table else None


def daily_sum(sql, relation, columns):
    if (
        not isinstance(sql, str)
        or not sql.lstrip().lower().startswith("select")
        or len(sql.encode()) > QUERY_BYTES
    ):
        return None
    try:
        statements = sqlglot.parse(sql, read="duckdb")
        if len(statements) != 1:
            return None
        query = statements[0]
        if type(query) is not exp.Select or not allowed_args(
            query, "expressions", "from_", "where", "group", "order"
        ):
            return None
        source = query.args.get("from_")
        table = source.this if source else None
        if (
            type(table) is not exp.Table
            or not allowed_args(table, "catalog", "db", "this")
            or not all(map(simple_identifier, table.parts))
            or ".".join(part.name.lower() for part in table.parts) != relation
            or len(query.expressions) != 2
        ):
            return None
        day, total = query.expressions
        if (
            not all(
                type(node) is exp.Alias and simple_identifier(node.args.get("alias"))
                for node in (day, total)
            )
            or day.alias.lower() == total.alias.lower()
        ):
            return None
        formatted, summed = day.this, total.this
        if (
            type(formatted) is not exp.TimeToStr
            or not allowed_args(formatted, "this", "format")
            or formatted.args.get("format") != exp.Literal.string(DATE_FORMAT)
            or type(summed) is not exp.Sum
            or not allowed_args(summed, "this")
        ):
            return None
        day_column, measure = column(formatted.this), column(summed.this)
        measure_type = columns.get(measure, "")
        if columns.get(day_column) != "DATE" or not (
            measure_type in {"TINYINT", "SMALLINT", "INTEGER", "BIGINT"}
            or measure_type.startswith("DECIMAL(")
        ):
            return None
        group, order = query.args.get("group"), query.args.get("order")
        if (
            type(group) is not exp.Group
            or not allowed_args(group, "expressions")
            or group.expressions != [exp.Literal.number(1)]
            or type(order) is not exp.Order
            or not allowed_args(order, "expressions")
            or len(order.expressions) != 1
        ):
            return None
        ordered = order.expressions[0]
        if (
            type(ordered) is not exp.Ordered
            or ordered.this != exp.Literal.number(1)
            or ordered.args.get("desc")
            or not allowed_args(ordered, "this", "desc", "nulls_first")
        ):
            return None
        where = query.args.get("where")
        if type(where) is not exp.Where:
            return None
        predicate = where.this.unnest()
        filter_column, value = "", None
        if type(predicate) is exp.And:
            terms = [predicate.this.unnest(), predicate.expression.unnest()]
            if type(terms[0]) is exp.Between:
                terms.reverse()
            extra, predicate = terms
            if type(extra) is exp.EQ:
                filter_column = column(extra.this)
                literal = extra.expression
                if (
                    columns.get(filter_column) != "VARCHAR"
                    or type(literal) is not exp.Literal
                    or not literal.is_string
                ):
                    return None
                value = literal.name
            elif extra != exp.true():
                return None
        if (
            type(predicate) is not exp.Between
            or not allowed_args(predicate, "this", "low", "high")
            or column(predicate.this) != day_column
        ):
            return None
        low_anchor, low = date_offset(predicate.args["low"].unnest())
        high_anchor, high = date_offset(predicate.args["high"].unnest())
        if low_anchor != high_anchor or low < high:
            return None
        return DailySum(
            day_column, measure, day.alias, total.alias, filter_column, low, high
        ), value
    except (sqlglot.errors.SqlglotError, RecursionError, ValueError):
        return None


def discover_daily_sums(events, coverage, relation, columns):
    groups = defaultdict(list)
    for _, duration, event in events:
        if match := daily_sum(event.get("sql"), relation, columns):
            shape, value = match
            groups[shape].append((duration, value))
    recurring = [shape for shape, rows in groups.items() if len(rows) >= 3]
    ranked = sorted(
        recurring, key=lambda shape: (-sum(t for t, _ in groups[shape]), shape)
    )[:8]
    workload = {}
    evidence = []
    for index, shape in enumerate(ranked):
        query_id = f"daily-{index}"
        rows = groups[shape]
        values = [value for value, _ in Counter(v for _, v in rows).most_common(4)]
        workload[query_id] = shape, values
        evidence.append(
            dict(
                id=query_id,
                executions=len(rows),
                median_ms=statistics.median(t for t, _ in rows),
            )
        )
    return workload, {
        **coverage,
        "family": "daily_sum",
        "matching_events": sum(s["executions"] for s in evidence),
        "queries": evidence,
    }


def daily_workload(args, relation, workload, db):
    if not 1 <= len(args.as_of) <= 8:
        raise ValueError("daily SUM replay requires one to eight archive anchors")
    source = ".".join(map(quote_identifier, relation.split(".")))
    baseline, rewritten, examples, cases = [], [], [], []
    for query_id, (shape, values) in workload.items():
        day, measure, day_label, sum_label = map(quote_identifier, shape[:4])
        bind = ["anchor", "anchor"]
        where = f"{day} BETWEEN ?::DATE - {shape.first_days_ago} AND ?::DATE - {shape.last_days_ago}"
        if shape.filter_column:
            where = f"{quote_identifier(shape.filter_column)} = ? AND {where}"
            bind.insert(0, "filter")
        before = f"SELECT strftime({day}, '{DATE_FORMAT}') AS {day_label}, SUM({measure}) AS {sum_label} FROM data WHERE {where} GROUP BY 1 ORDER BY 1"
        after = (
            f"SELECT strftime(day, '{DATE_FORMAT}') AS {day_label}, total AS {sum_label} FROM "
            f"(SELECT {day} AS day, SUM({measure}) AS total FROM {{source}} WHERE {where} GROUP BY {day}) daily "
            "ORDER BY 1"
        )
        baseline.append(dict(id=query_id, sql=before, bind=bind))
        rewritten.append(dict(id=query_id, sql=after.format(source="data"), bind=bind))
        parameter_types = {"anchor": "DATE"}
        if shape.filter_column:
            parameter_types["filter"] = "VARCHAR"
        examples.append(
            dict(
                id=query_id,
                sql=after.format(source=source),
                bind=bind,
                parameters=parameter_types,
            )
        )
        for anchor in args.as_of:
            for value in values:
                params = {"anchor": anchor.isoformat(), "filter": value}
                if db.execute(
                    f"SELECT 1 FROM read_parquet(?) WHERE {where} LIMIT 1",
                    [str(args.export), *(params[name] for name in bind)],
                ).fetchone():
                    cases.append(
                        dict(
                            id=f"window-{len(cases)}",
                            params={name: params[name] for name in bind},
                            query_ids=[query_id],
                        )
                    )
        if not any(case["query_ids"] == [query_id] for case in cases):
            raise ValueError(
                "daily SUM replay requires data for each query shape in the selected archive windows"
            )
    config = dict(
        snapshot=args.snapshot,
        baseline=baseline,
        candidates=[dict(id=STRATEGY, queries=rewritten)],
        cases=cases,
        ordered=True,
        repetitions=args.repetitions,
    )
    contract = dict(
        verified_on_export=args.snapshot, source_relation=relation, queries=examples
    )
    return config, contract


def daily_payoff(candidate, history, minimum):
    result = {"minimum_net_ms_per_day": minimum}
    if candidate["status"] != "faster_on_replay":
        return dict(result, gate="replay_did_not_pass")
    case_savings = defaultdict(list)
    for case in candidate["cases"]:
        (query_id,) = case["outputs"]
        case_savings[query_id].append(
            case["median_saved_ms"] / case["baseline"]["median_ms"]
        )
    net = (
        sum(
            query["executions"] * query["median_ms"] * min(case_savings[query["id"]])
            for query in history["queries"]
        )
        / history["window_days"]
    )
    return dict(
        result,
        gate="passes" if net >= minimum else "insufficient_estimated_payoff",
        estimated_saved_ms_per_day=net,
        estimated_net_ms_per_day=net,
        estimated_refresh_ms_per_day=0,
        amortized_build_ms_per_day=0,
    )
