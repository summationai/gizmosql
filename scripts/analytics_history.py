# Licensed under the Apache License, Version 2.0.
"""Read bounded query history from GizmoSQL instrumentation exports or legacy logs."""

import json
from datetime import UTC, datetime, timedelta

import duckdb

HISTORY_BYTES = 8 * 1024 * 1024
HISTORY_ROWS = 4096
QUERY_BYTES = 16 * 1024


def log_events(path):
    with path.open("rb") as handle:
        offset = max(0, handle.seek(0, 2) - HISTORY_BYTES)
        handle.seek(offset)
        lines = handle.read(HISTORY_BYTES).split(b"\n")
    # A file can rotate or end while the server is writing a record.
    for raw in lines[bool(offset) : -1]:
        try:
            event = json.loads(raw)
            if isinstance(event, dict):
                yield event
        except (RecursionError, ValueError):
            continue


def instrumentation_events(path, methods, *, start=None, end=None, user=None, hits=False):
    """Read a private Parquet export of joined sql_executions and sql_statements rows."""
    filters = [
        "status = 'success'",
        "NOT is_internal",
        "NOT has_binds",
        "flight_method IN (SELECT unnest(?::VARCHAR[]))",
        "execution_start_time IS NOT NULL",
        "duration_ms >= 0",
        "strlen(sql_text) BETWEEN 1 AND ?",
    ]
    parameters = [str(path), sorted(methods), QUERY_BYTES]
    if not hits:
        filters.append("NOT coalesce(materialization_hit, false)")
    for value, predicate in (
        (user, "username = ?"),
        (start, "execution_start_time >= ?::TIMESTAMPTZ"),
        (end, "execution_start_time <= ?::TIMESTAMPTZ"),
    ):
        if value is not None:
            filters.append(predicate)
            parameters.append(value)
    where = " AND ".join(filters)
    with duckdb.connect(config={"threads": 1, "memory_limit": "256MB"}) as db:
        rows = db.execute(
            f"""
            SELECT epoch_us(execution_start_time), duration_ms, sql_text, flight_method,
                   session_id::VARCHAR, instance_id::VARCHAR, username,
                   coalesce(materialization_hit, false)
            FROM read_parquet(?) WHERE {where}
            ORDER BY execution_start_time DESC, execution_id DESC LIMIT ?
            """,
            [*parameters, HISTORY_ROWS],
        )
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        for (
            micros,
            duration,
            sql,
            method,
            session,
            instance,
            username,
            hit,
        ) in rows.fetchall():
            yield {
                "ts": (epoch + timedelta(microseconds=micros)).isoformat(),
                "duration_ms": duration,
                "sql": sql,
                "flight_method": method,
                "session_id": session,
                "instance_id": instance,
                "user": username,
                "kind": "analytics" if hit else "sql",
                "status": "hit" if hit else "success",
                "is_internal": False,
                "direct_execution": False,
            }
