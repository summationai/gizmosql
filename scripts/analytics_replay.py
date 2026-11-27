#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.
"""Benchmark candidate SQL and layouts on a local Parquet export."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import pickle
import re
import statistics
import tempfile
import time
from uuid import uuid4

import duckdb

from analytics_publication import atomic_write


MAX_ROWS = 10_000
NAME = re.compile(r"[a-zA-Z0-9_-]{1,64}")


def select_sql(db, sql):
    statements = db.extract_statements(sql)
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
        raise ValueError("expected a single SELECT")
    return statements[0].query


def collect(db, queries, params):
    """Fetch every result before timing stops. Split batched results afterward."""
    start = time.perf_counter()
    batches = []
    for query in queries:
        result = db.execute(query["sql"], [params[name] for name in query.get("bind", [])])
        schema = tuple((col[0], str(col[1])) for col in result.description)
        rows = result.fetchmany(MAX_ROWS + 1)
        if len(rows) > MAX_ROWS:
            raise ValueError("result exceeds benchmark row limit")
        batches.append((query, schema, rows))
    elapsed_ms = (time.perf_counter() - start) * 1000
    outputs = {}
    for query, schema, rows in batches:
        columns = {name: (index, dtype) for index, (name, dtype) in enumerate(schema)}
        if len(columns) != len(schema):
            raise ValueError("result column names must be unique")
        split = query.get("outputs", {query.get("id", "result"): {name: name for name in columns}})
        for name, projection in split.items():
            if name in outputs:
                raise ValueError("logical output IDs must be unique")
            indexes = [columns[column][0] for column in projection.values()]
            projected_schema = tuple((alias, columns[column][1]) for alias, column in projection.items())
            projected_rows = [tuple(row[index] for index in indexes) for row in rows]
            outputs[name] = (projected_schema, projected_rows)
    return elapsed_ms, outputs


def comparable(outputs, ordered):
    # Compare types and complete rows. A multiset retains duplicates and handles
    # nested values without relying on SQL coercions or a sort order.
    return {
        name: (schema, rows if ordered else Counter(pickle.dumps(row) for row in rows))
        for name, (schema, rows) in outputs.items()
    }


def compile_queries(db, queries):
    if not queries:
        raise ValueError("query list is empty")
    return [dict(query, sql=select_sql(db, query["sql"])) for query in queries]


def timings(samples):
    return {
        "samples": len(samples),
        "median_ms": statistics.median(samples),
        "p90_ms": statistics.quantiles(samples, n=10, method="inclusive")[-1],
    }


def compare_case(baseline, candidate, original, rewritten, case, rounds, ordered, build_ms):
    params = case.get("params", {})
    _, outputs = collect(baseline, original, params)
    expected = comparable(outputs, ordered)
    case_report = {
        "id": case["id"],
        "outputs": {key: {"schema": schema, "rows": len(rows)} for key, (schema, rows) in outputs.items()},
    }
    if comparable(collect(candidate, rewritten, params)[1], ordered) != expected:
        return dict(case_report, status="mismatch")
    samples = [[], []]
    paths = [(baseline, original), (candidate, rewritten)]
    for round_id in range(rounds):
        for index in (range(2) if round_id % 2 == 0 else (1, 0)):
            elapsed, result = collect(*paths[index], params)
            if comparable(result, ordered) != expected:
                return dict(case_report, status="unstable" if index == 0 else "mismatch")
            samples[index].append(elapsed)
    before, after = map(timings, samples)
    saved = before["median_ms"] - after["median_ms"]
    return dict(case_report, status="equivalent", baseline=before, candidate=after,
                median_saved_ms=saved,
                break_even_reads=math.ceil(build_ms / saved) if saved > 0 else None)


def evaluate(db, baseline, original, scenario, cases, rounds, ordered):
    result = {"id": scenario["id"], "strategy": "rewrite", "build_ms": 0, "cases": []}
    phase = "build"
    try:
        with db.cursor() as candidate:
            if "build_sql" in scenario and "sort_by" in scenario:
                raise ValueError("specify build_sql or sort_by")
            start = time.perf_counter()
            if "build_sql" in scenario:
                result["strategy"] = "derived_table"
                query = select_sql(candidate, scenario["build_sql"])
                candidate.execute("CREATE TEMP TABLE replay_artifact AS " + query)
                candidate.execute("CREATE TEMP VIEW data AS SELECT * FROM replay_artifact")
            elif "sort_by" in scenario:
                result["strategy"] = "sorted_copy"
                names = set(row[0] for row in candidate.execute("DESCRIBE source").fetchall())
                columns = scenario["sort_by"]
                if not columns or any(col not in names for col in columns):
                    raise ValueError("sort_by must name source columns")
                order = ", ".join('"' + col.replace('"', '""') + '"' for col in columns)
                candidate.execute("CREATE TEMP TABLE data AS FROM source ORDER BY " + order)
            else:
                candidate.execute("CREATE TEMP VIEW data AS FROM source")
            result["build_ms"] = (time.perf_counter() - start) * 1000
            phase = "replay"
            rewritten = compile_queries(candidate, scenario["queries"]) if "queries" in scenario else original
            for case in cases:
                result["cases"].append(compare_case(
                    baseline, candidate, original, rewritten, case, rounds, ordered, result["build_ms"]))
            checks = result["cases"]
            if any(c["status"] != "equivalent" for c in checks):
                result["status"] = "rejected"
            elif all(c["median_saved_ms"] > 0 and c["candidate"]["p90_ms"] < c["baseline"]["p90_ms"]
                     for c in checks):
                result["status"] = "faster_on_replay"
            else:
                result["status"] = "equivalent"
    except (duckdb.Error, KeyError, TypeError, ValueError) as error:
        result.update(status="error", phase=phase, error=type(error).__name__)
    return result


def validate_ids(entries):
    ids = [entry["id"] for entry in entries]
    if not ids or len(set(ids)) != len(ids) or not all(NAME.fullmatch(value) for value in ids):
        raise ValueError("expected distinct, nonempty case or candidate IDs")


def replay(config, parquet, *, include_workload=False):
    rounds = config.get("repetitions", 8)
    if type(rounds) is not int or not 2 <= rounds <= 100:
        raise ValueError("repetitions must be between 2 and 100")
    cases, scenarios = config["cases"], config["candidates"]
    validate_ids(cases)
    validate_ids(scenarios)
    original_stat = parquet.stat()
    with parquet.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    # The ingest statement is the only SQL allowed filesystem access. Candidate
    # SQL runs in connection-local scratch state with external access locked.
    with tempfile.TemporaryDirectory(prefix="gizmosql-replay-") as scratch:
        with duckdb.connect(config={"threads": 2, "memory_limit": "512MB",
                                    "max_temp_directory_size": "2GB",
                                    "temp_directory": scratch}) as db:
            db.execute("CREATE TABLE source AS SELECT * FROM read_parquet(?)", [str(parquet)])
            imported_stat = parquet.stat()
            if (original_stat.st_ino, original_stat.st_size, original_stat.st_mtime_ns) != (
                imported_stat.st_ino, imported_stat.st_size, imported_stat.st_mtime_ns
            ):
                raise ValueError("export changed during import")
            rows = db.execute("SELECT count(*) FROM source").fetchone()[0]
            db.execute("SET enable_external_access=false; SET lock_configuration=true")
            with db.cursor() as baseline:
                baseline.execute("CREATE TEMP VIEW data AS FROM source")
                original = compile_queries(baseline, config["baseline"])
                candidates = [evaluate(db, baseline, original, candidate, cases, rounds,
                                       config.get("ordered", False)) for candidate in scenarios]
    report = {
        "format": 1, "id": uuid4().hex, "created_at": datetime.now(timezone.utc).isoformat(),
        "dataset": {"snapshot": config["snapshot"], "rows": rows, "sha256": digest},
        "engine": {"name": "DuckDB", "version": duckdb.__version__, "transport": "in_process"},
        "candidates": candidates,
    }
    if include_workload:
        report["workload"] = {key: config[key] for key in ("baseline", "cases", "candidates")}
        report["workload"].update(repetitions=rounds, ordered=config.get("ordered", False))
    return report


def write_report(report, report_dir):
    report_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = report_dir / (report["id"] + ".json")
    atomic_write(path, lambda stream: stream.write(json.dumps(report, indent=2).encode()))


def run(config, parquet, report_dir, *, include_workload=False):
    report = replay(config, parquet, include_workload=include_workload)
    write_report(report, report_dir)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", required=True, type=Path, help="local Parquet file")
    parser.add_argument("--workload", required=True, type=Path, help="trusted local workload JSON")
    parser.add_argument("--reports", required=True, type=Path)
    parser.add_argument("--include-workload", action="store_true", help="include SQL and parameters in reports")
    args = parser.parse_args()
    parquet = args.export.resolve(strict=True)
    if not parquet.is_file():
        parser.error("export must be a local file")
    report = run(json.loads(args.workload.read_text()), parquet, args.reports,
                 include_workload=args.include_workload)
    print(json.dumps({"id": report["id"], "candidates": {c["id"]: c["status"] for c in report["candidates"]}}))


if __name__ == "__main__":
    main()
