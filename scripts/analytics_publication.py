# Licensed under the Apache License, Version 2.0.
"""Publish immutable Arrow results and exact-query manifests for the server."""

import hashlib
import hmac
import json
import logging
import os
import tempfile
from pathlib import Path

import pyarrow as pa

import duckdb

MAX_IPC_BYTES = 8 * 1024 * 1024


def atomic_write(path, write):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            write(handle)
            handle.flush()
            handle.close()
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def write_arrow(db, name, path):
    reader = db.execute(f'SELECT * FROM main."{name}"').to_arrow_reader(batch_size=4096)
    if not all(
        pa.types.is_integer(field.type) or pa.types.is_decimal(field.type)
        for field in reader.schema
    ):
        return False  # restrict serving to exact numeric results in the pilot
    size = 0
    try:

        def write(handle):
            nonlocal size
            with pa.ipc.new_file(handle, reader.schema) as output:
                for batch in reader:
                    size += batch.nbytes
                    if size > MAX_IPC_BYTES:
                        raise OverflowError("materialization exceeds the IPC budget")
                    output.write_batch(batch)
            if handle.tell() > MAX_IPC_BYTES:
                raise OverflowError("materialization exceeds the IPC budget")

        atomic_write(path, write)
    except OverflowError:
        return False
    return True


def publish(db, directory, key, args, selected, metadata_catalog, metadata_schema):
    """Publish one recent exact SQL spelling per selected materialization."""
    directory.mkdir(mode=0o700, exist_ok=True)
    manifests, blobs = set(), set()
    for name, (definition, raw_sql, catalog) in selected.items():
        row = db.execute(
            """
            SELECT m.source_epoch, m.snapshot_id, m.snapshot_time_us, epoch_us(m.refreshed_at),
                   s.snapshot_id, s.snapshot_time_us, s.schema_version, p.profile
            FROM main.materializations m JOIN main.materialization_refresh_state s USING (name)
            JOIN main.materialization_build_profile p USING (name)
            WHERE m.name = ? AND m.definition = ?
        """,
            [name, definition],
        ).fetchone()
        if not row or row[0] != args.source_epoch:
            continue
        generation = hashlib.sha256(repr(row[:4]).encode()).hexdigest()[:32]
        blob = directory / f"{name}_{generation}.arrow"
        if not blob.exists():
            try:
                if not write_arrow(db, name, blob):
                    continue
            except (duckdb.Error, pa.ArrowException) as error:
                logging.warning("could not export %s (%s)", name, type(error).__name__)
                continue
        match = hmac.new(
            key,
            b"query\0" + args.user.encode() + b"\0" + raw_sql.encode(),
            hashlib.sha256,
        ).hexdigest()
        manifest = directory / f"match_{match}.json"
        payload = {
            "format": 2,
            "artifact": blob.name,
            "catalog": catalog,
            "epoch": row[0],
            "metadata_catalog": metadata_catalog,
            "metadata_schema": metadata_schema,
            "snapshot": list(row[4:7]),
            **json.loads(row[7]),
        }
        encoded = json.dumps(payload, separators=(",", ":")).encode()
        if not manifest.exists() or manifest.read_bytes() != encoded:
            atomic_write(manifest, lambda handle: handle.write(encoded))
        manifests.add(manifest)
        blobs.add(blob)
    for pattern, live in (("match_*.json", manifests), ("mv_*.arrow", blobs)):
        for path in directory.glob(pattern):
            if path not in live:
                path.unlink(missing_ok=True)
