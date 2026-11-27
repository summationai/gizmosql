#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.
"""Build private DuckLake query materializations from GizmoSQL JSON query logs."""

import argparse
import hashlib
import hmac
import json
import logging
import os
import stat
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from analytics_lineage import (
    IDENTIFIER,
    MAX_HISTORY_SNAPSHOTS,
    Edge,
    candidate,
    catalog_lineage,
    changes_affect,
    metadata_location,
    quote_identifier,
)


HISTORY_BYTES = 8 * 1024 * 1024
QUERY_BYTES = 16 * 1024
SETTINGS_SQL = """
    SELECT string_agg(lower(name) || '=' || hex(value), ',' ORDER BY lower(name))
    FROM duckdb_settings()
    WHERE lower(name) IN ('timezone', 'calendar', 'default_collation', 'old_implicit_casting',
                          'arrow_output_version', 'arrow_lossless_conversion', 'arrow_large_buffer_size')
"""


class SnapshotHistoryUnavailable(Exception):
    pass


def recent_queries(log_path, user, history, now):
    counts, elapsed, hits = Counter(), Counter(), Counter()
    with log_path.open("rb") as log:
        offset = max(0, log.seek(0, os.SEEK_END) - HISTORY_BYTES)
        log.seek(offset)
        lines = log.read(HISTORY_BYTES).split(b"\n")
    # Do not count partial lines at either end of the scan.
    for line in lines[bool(offset):-1]:
        try:
            event = json.loads(line)
            if event.get("user") != user:
                continue
            timestamp = datetime.fromisoformat(event["ts"])
            if not now - history <= timestamp <= now:
                continue
            sql = event["sql"]
            if not isinstance(sql, str) or len(sql.encode()) > QUERY_BYTES:
                continue
            if event.get("kind") == "analytics" and event.get("status") == "hit":
                hits[sql] += 1
            elif (event.get("kind") == "sql" and event.get("status") == "success"
                  and event.get("is_internal") in (False, "false")
                  and event.get("direct_execution") in (False, "false")
                  and event.get("flight_method") == "DoGetStatement"):
                duration = int(event["duration_ms"])
                if duration >= 0:
                    counts[sql] += 1
                    elapsed[sql] += duration
        except (AttributeError, KeyError, OverflowError, RecursionError, TypeError, ValueError):
            continue
    return counts, elapsed, hits


def init_store(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS main.materializations (
            name VARCHAR PRIMARY KEY, source_table VARCHAR NOT NULL,
            definition VARCHAR NOT NULL, source_epoch VARCHAR NOT NULL,
            snapshot_id BIGINT NOT NULL, snapshot_time_us BIGINT NOT NULL,
            schema_version BIGINT NOT NULL, refreshed_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS main.materialization_dependencies (
            name VARCHAR NOT NULL, dependent VARCHAR NOT NULL, source_relation VARCHAR NOT NULL,
            source_kind VARCHAR NOT NULL, source_id BIGINT NOT NULL, source_uuid VARCHAR NOT NULL,
            PRIMARY KEY (name, dependent, source_relation)
        );
        CREATE TABLE IF NOT EXISTS main.materialization_refresh_state (
            name VARCHAR PRIMARY KEY, snapshot_id BIGINT NOT NULL,
            snapshot_time_us BIGINT NOT NULL, schema_version BIGINT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS main.materialization_build_profile (
            name VARCHAR PRIMARY KEY, profile VARCHAR NOT NULL
        )
    """)


def refresh(db, name, definition, roots, args, metadata, *, check_history=True):
    catalog = quote_identifier(roots[0][0])
    db.begin()
    try:
        # First source access pins this transaction's DuckLake snapshot, before CTAS.
        snapshot_id = db.execute(f"FROM {catalog}.current_snapshot()").fetchone()[0]
        snapshot = db.execute(f"""
            SELECT snapshot_id, epoch_us(snapshot_time), schema_version
            FROM {metadata}.ducklake_snapshot WHERE snapshot_id = ?
        """, [snapshot_id]).fetchone()
        if snapshot is None:
            raise ValueError("expected a committed source snapshot")
        edges = catalog_lineage(db, metadata, snapshot_id, name, roots, args.source_table, args.source_view)
        engine, path, extension = db.execute("""
            SELECT version(), data_path, extension_version FROM ducklake_settings(?)
        """, [roots[0][0]]).fetchone()
        profile = json.dumps({"engine": engine, "data_path": path, "extension": extension,
                              "settings": db.execute(SETTINGS_SQL).fetchone()[0]})
        current = db.execute("""
            SELECT m.source_epoch, s.snapshot_id, s.snapshot_time_us, s.schema_version
            FROM main.materializations m JOIN main.materialization_refresh_state s USING (name)
            JOIN main.materialization_build_profile p USING (name)
            WHERE m.name = ? AND p.profile = ?
        """, [name, profile]).fetchone()
        previous_edges = {Edge(*row) for row in db.execute("""
            SELECT dependent, source_relation, source_kind, source_id, source_uuid
            FROM main.materialization_dependencies WHERE name = ?
        """, [name]).fetchall()}
        if check_history and current and current[0] == args.source_epoch and previous_edges == edges:
            anchor = current[1:]
            if anchor == snapshot:
                db.rollback()
                return
            if 0 < snapshot_id - anchor[0] <= MAX_HISTORY_SNAPSHOTS:
                try:
                    history = db.execute(f"""
                        SELECT snapshot_id, epoch_us(snapshot_time), schema_version, changes
                        FROM {catalog}.snapshots()
                        WHERE snapshot_id BETWEEN ? AND ? ORDER BY snapshot_id
                    """, [anchor[0], snapshot_id]).fetchall()
                except duckdb.Error as error:
                    # Older extensions can reject unknown change types during binding.
                    raise SnapshotHistoryUnavailable from error
                if not changes_affect(history, anchor, snapshot, edges):
                    record_checked_snapshot(db, name, snapshot)
                    db.commit()
                    return
        # LIMIT bounds the stored result. Rollback preserves the prior generation on failure.
        db.execute(f'CREATE OR REPLACE TABLE main."{name}" AS '
                   f'SELECT * FROM ({definition}) AS materialized LIMIT {args.max_rows + 1}')
        if db.execute(f'SELECT count(*) FROM main."{name}"').fetchone()[0] > args.max_rows:
            raise ValueError("materialized result exceeds row limit")
        db.execute("""
            INSERT OR REPLACE INTO main.materializations
            VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp)
        """, [name, ".".join(roots[0]), definition, args.source_epoch, *snapshot])
        db.execute("DELETE FROM main.materialization_dependencies WHERE name = ?", [name])
        db.executemany("INSERT INTO main.materialization_dependencies VALUES (?, ?, ?, ?, ?, ?)",
                       [(name, *edge) for edge in edges])
        record_checked_snapshot(db, name, snapshot)
        db.execute("INSERT OR REPLACE INTO main.materialization_build_profile VALUES (?, ?)", [name, profile])
        db.commit()
    except (duckdb.Error, ValueError, SnapshotHistoryUnavailable):
        db.rollback()
        raise
    logging.info("published %s at catalog snapshot %s", name, snapshot_id)


def record_checked_snapshot(db, name, snapshot):
    db.execute("INSERT OR REPLACE INTO main.materialization_refresh_state VALUES (?, ?, ?, ?)",
               [name, *snapshot])


def reconcile(db, key, args, metadata_catalog, metadata_schema):
    metadata = ".".join(map(quote_identifier, (metadata_catalog, metadata_schema)))
    counts, elapsed, hits = recent_queries(args.query_log, args.user,
                                           timedelta(seconds=args.history_seconds), datetime.now(UTC))
    demand = counts + hits
    totals, durations, reuse, tables, spellings = Counter(), Counter(), Counter(), {}, {}
    allowed_relations = args.source_table | args.source_view
    for sql in demand:
        eligible = candidate(sql, allowed_relations)
        if eligible:
            definition, source = eligible
            tables[definition] = source
            prior = spellings.get(definition)
            if prior is None or (demand[sql], sql) > (demand[prior], prior):
                spellings[definition] = sql
            totals[definition] += counts[sql]
            durations[definition] += elapsed[sql]
            reuse[definition] += hits[sql]
    qualified = {sql for sql, count in totals.items()
                 if count >= args.min_runs and durations[sql] >= count * args.min_duration_ms}
    ranked = sorted(qualified,
                    key=lambda sql: (-durations[sql], -totals[sql], sql))
    existing = dict(db.execute("SELECT name, definition FROM main.materializations").fetchall())
    # Hits retain already admitted work without treating cache lookup time as source cost.
    retained = sorted({definition for definition in existing.values() if reuse[definition]},
                      key=lambda definition: (-reuse[definition], definition))
    selected = {}
    for definition in dict.fromkeys(ranked + retained):
        if len(selected) >= args.max_views:
            break
        identity = f"{args.user}\0{definition}".encode()
        name = "mv_" + hmac.new(key, identity, hashlib.sha256).hexdigest()
        if definition in qualified or existing.get(name) == definition:
            selected[name] = definition
    for name in existing:
        if name not in selected:
            db.begin()
            try:
                db.execute(f'DROP TABLE IF EXISTS main."{name}"')
                db.execute("DELETE FROM main.materialization_dependencies WHERE name = ?", [name])
                db.execute("DELETE FROM main.materialization_refresh_state WHERE name = ?", [name])
                db.execute("DELETE FROM main.materialization_build_profile WHERE name = ?", [name])
                db.execute("DELETE FROM main.materializations WHERE name = ?", [name])
                db.commit()
            except duckdb.Error:
                db.rollback()
                raise
    failures = 0
    ready = {}
    for name, definition in selected.items():
        try:
            try:
                refresh(db, name, definition, tables[definition], args, metadata)
            except SnapshotHistoryUnavailable:
                logging.info("snapshot history unavailable for %s, rebuilding", name)
                refresh(db, name, definition, tables[definition], args, metadata, check_history=False)
        except (duckdb.Error, ValueError) as error:
            logging.warning("refresh failed for %s (%s)", name, type(error).__name__)
            failures += 1
        else:
            ready[name] = (definition, spellings[definition], tables[definition][0][0])
    if args.publish:
        from analytics_publication import publish

        publish(db, args.state_dir / "published", key, args, ready,
                metadata_catalog, metadata_schema)
    return failures


def positive_int(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return result


def source_table(value):
    parts = tuple(value.lower().split("."))
    if len(parts) != 3 or not all(map(IDENTIFIER.fullmatch, parts)):
        raise argparse.ArgumentTypeError("use catalog.schema.table with simple identifiers")
    return parts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-log", required=True, type=Path)
    parser.add_argument("--source-init-sql", required=True, type=Path,
                        help="trusted SQL file that attaches the DuckLake source READ_ONLY")
    parser.add_argument("--source-table", required=True, type=source_table, action="append")
    parser.add_argument("--source-view", type=source_table, action="append", default=[],
                        help="approved DuckLake view, including intermediate views")
    parser.add_argument("--metadata-catalog", help="override the DuckLake metadata catalog alias")
    parser.add_argument("--metadata-schema", help="select the DuckLake metadata schema when ambiguous")
    parser.add_argument("--source-epoch", required=True, help="change after catalog replacement/restore")
    parser.add_argument("--user", required=True, help="single trusted query-log username")
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--min-runs", type=positive_int, default=3)
    parser.add_argument("--min-duration-ms", type=int, default=100)
    parser.add_argument("--history-seconds", type=positive_int, default=3600)
    parser.add_argument("--poll-seconds", type=positive_int, default=30)
    parser.add_argument("--max-views", type=positive_int, default=8)
    parser.add_argument("--max-rows", type=positive_int, default=100_000)
    parser.add_argument("--publish", action="store_true", help="publish Arrow results for server lookup")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    args.source_table, args.source_view = set(args.source_table), set(args.source_view)
    if args.source_table & args.source_view:
        parser.error("a source relation must be listed as either a table or a view")
    catalogs = {relation[0] for relation in args.source_table | args.source_view}
    if len(catalogs) != 1 or args.min_duration_ms < 0:
        parser.error("use one source catalog and a nonnegative duration threshold")
    args.state_dir.mkdir(mode=0o700, exist_ok=True)
    state = args.state_dir.lstat()
    if not stat.S_ISDIR(state.st_mode) or state.st_uid != os.getuid() or state.st_mode & 0o077:
        parser.error("state directory must be private and owned by the worker user")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        with duckdb.connect(str(args.state_dir / "artifacts.duckdb"),
                            config={"threads": 1, "memory_limit": "512MB",
                                    "max_temp_directory_size": "1GB"}) as db:
            key_path = args.state_dir / "identity.key"
            if not key_path.exists():
                with os.fdopen(os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as key_file:
                    key_file.write(os.urandom(32))
            key = key_path.read_bytes()
            if len(key) != 32:
                parser.error("state directory contains an invalid identity key")
            db.execute(args.source_init_sql.read_text())
            catalog = next(iter(catalogs))
            attached = db.execute("""
                SELECT type, readonly FROM duckdb_databases() WHERE lower(database_name) = ?
            """, [catalog]).fetchone()
            if attached != ("ducklake", True):
                parser.error("source SQL must attach the specified DuckLake catalog READ_ONLY")
            try:
                metadata = metadata_location(db, catalog, args.metadata_catalog, args.metadata_schema)
            except ValueError as error:
                parser.error(str(error))
            init_store(db)
            while True:
                try:
                    failures = reconcile(db, key, args, *metadata)
                except FileNotFoundError:
                    logging.warning("query log is unavailable")
                    failures = 1
                if args.once:
                    return int(failures > 0)
                time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        return 0
    except (duckdb.Error, OSError, UnicodeError) as error:
        logging.error("worker stopped (%s)", type(error).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
