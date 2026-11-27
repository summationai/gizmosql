# Licensed under the Apache License, Version 2.0.
"""Validate query relations and resolve their DuckLake lineage at one snapshot."""

import re
from typing import NamedTuple

import sqlglot
from sqlglot import exp


IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z_0-9]*\Z")
AGGREGATES = (exp.Count, exp.Sum, exp.Min, exp.Max)
COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)
MAX_LINEAGE_RELATIONS = 64
MAX_HISTORY_SNAPSHOTS = 1024

# These are keys returned by DuckLake snapshots(), not the raw metadata labels.
TABLE_CHANGES = frozenset({
    "tables_altered", "tables_dropped", "tables_inserted_into", "tables_deleted_from",
    "inlined_insert", "inlined_delete", "flushed_inlined", "merge_adjacent", "rewrite_delete",
})
VIEW_CHANGES = frozenset({"views_altered", "views_dropped"})
NAMESPACE_CHANGES = frozenset({"schemas_created", "schemas_dropped", "tables_created", "views_created"})


class Edge(NamedTuple):
    dependent: str
    source_relation: str
    source_kind: str
    source_id: int
    source_uuid: str


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def simple_identifier(node):
    return type(node) is exp.Identifier and IDENTIFIER.fullmatch(node.name)


def allowed_args(node, *allowed):
    return all(not value or key in allowed for key, value in node.args.items())


def simple_column(node):
    return (type(node) is exp.Column and allowed_args(node, "this", "table")
            and simple_identifier(node.this)
            and (not node.table or simple_identifier(node.args.get("table"))))


def simple_predicate(node):
    pending = [node]
    while pending:
        term = pending.pop().unnest()
        if type(term) is exp.And:
            pending.extend((term.this, term.expression))
        elif (type(term) not in COMPARISONS or not simple_column(term.this)
              or type(term.expression) not in (exp.Literal, exp.Boolean)):
            return False
    return True


def input_relations(query):
    if type(query) is not exp.Select or not allowed_args(
        query, "expressions", "from_", "joins", "where", "group"
    ):
        return None
    source = query.args.get("from_")
    if type(source) is not exp.From:
        return None
    tables = [source.this]
    for join in query.args.get("joins", ()):
        condition = join.args.get("on")
        condition = condition.unnest() if condition is not None else None
        if (not allowed_args(join, "this", "kind", "side", "on")
                or (join.side, join.kind) not in {("", ""), ("", "INNER"), ("LEFT", ""), ("LEFT", "OUTER")}
                or type(condition) is not exp.EQ
                or not all(map(simple_column, (condition.this, condition.expression)))):
            return None
        tables.append(join.this)
    relations = []
    for table in tables:
        if (type(table) is not exp.Table or not allowed_args(table, "catalog", "db", "this", "alias")
                or len(table.parts) != 3 or not all(map(simple_identifier, table.parts))):
            return None
        alias = table.args.get("alias")
        if alias is not None and (not allowed_args(alias, "this") or not simple_identifier(alias.this)):
            return None
        relations.append(tuple(part.name.lower() for part in table.parts))
    where = query.args.get("where")
    if where is not None and not simple_predicate(where.this):
        return None
    return tuple(dict.fromkeys(relations))


def checked_select(sql, allowed_relations, *, aggregate):
    try:
        statements = sqlglot.parse(sql, read="duckdb")
        if len(statements) != 1:
            return None
        query = statements[0]
        relations = input_relations(query)
        if not relations or any(relation not in allowed_relations for relation in relations):
            return None
        group = query.args.get("group")
        if group is not None and (not aggregate or not allowed_args(group, "expressions")
                                  or not all(map(simple_column, group.expressions))):
            return None
        group_columns = {(column.table.lower(), column.name.lower()) for column in group.expressions} if group else set()
        output_names = set()
        has_aggregate = False
        for projection in query.expressions:
            aliased = type(projection) is exp.Alias
            expression = projection.this if aliased else projection
            output = projection.args.get("alias") if aliased else expression.this
            if not simple_identifier(output) or output.name.lower() in output_names:
                return None
            output_names.add(output.name.lower())
            if simple_column(expression) and (
                not aggregate or (expression.table.lower(), expression.name.lower()) in group_columns
            ):
                continue
            if (not aggregate or type(expression) not in AGGREGATES or not aliased
                    or not allowed_args(expression, "this", "big_int")):
                return None
            argument = expression.this
            if not simple_column(argument) and not (type(expression) is exp.Count
                                                     and type(argument) is exp.Star
                                                     and not any(argument.args.values())):
                return None
            has_aggregate = True
        if aggregate and not has_aggregate:
            return None
        # Re-serialize the checked tree. Never send the log's raw SQL to DuckDB.
        for node in query.walk():
            node.comments = None
        return query.sql(dialect="duckdb", identify=True), relations
    except (sqlglot.errors.SqlglotError, RecursionError):
        return None


def candidate(sql, allowed_relations):
    return checked_select(sql, allowed_relations, aggregate=True)


def metadata_location(db, catalog, metadata_catalog, metadata_schema):
    database = metadata_catalog or f"__ducklake_metadata_{catalog}"
    schemas = db.execute("""
        SELECT t.schema_name FROM duckdb_tables() t
        JOIN duckdb_databases() d ON t.database_name = d.database_name
        WHERE lower(t.database_name) = lower(?) AND d.readonly
          AND (? IS NULL OR lower(t.schema_name) = lower(?))
          AND t.table_name IN ('ducklake_schema', 'ducklake_table', 'ducklake_view',
                               'ducklake_snapshot', 'ducklake_metadata')
        GROUP BY t.schema_name HAVING count(DISTINCT t.table_name) = 5
    """, [database, metadata_schema, metadata_schema]).fetchall()
    if len(schemas) != 1:
        raise ValueError("expected one read-only DuckLake metadata schema, or specify --metadata-catalog and --metadata-schema")
    prefix = f"{quote_identifier(database)}.{quote_identifier(schemas[0][0])}"
    expected = db.execute("SELECT data_path FROM ducklake_settings(?)", [catalog]).fetchone()
    configured = db.execute(f"""
        SELECT value FROM {prefix}.ducklake_metadata WHERE key = 'data_path' AND scope IS NULL
    """).fetchall()
    if configured != [expected]:
        raise ValueError("DuckLake metadata schema does not match the source data path")
    return database, schemas[0][0]


def catalog_lineage(db, metadata, snapshot_id, name, roots, allowed_tables, allowed_views):
    allowed_relations = allowed_tables | allowed_views
    resolved = {}
    visiting = set()
    edges = set()

    def resolve(relation, dependent):
        if relation in visiting:
            raise ValueError("cycle in view lineage")
        if relation not in resolved:
            if len(resolved) + len(visiting) >= MAX_LINEAGE_RELATIONS:
                raise ValueError("view lineage exceeds the relation limit")
            visiting.add(relation)
            _, schema, name = relation
            kind = "view" if relation in allowed_views else "table"
            extra = ', obj.dialect, obj.sql, obj.column_aliases' if kind == "view" else ''
            rows = db.execute(f"""
                SELECT obj.{kind}_id, CAST(obj.{kind}_uuid AS VARCHAR){extra}
                FROM {metadata}.ducklake_{kind} obj
                JOIN {metadata}.ducklake_schema s USING (schema_id)
                WHERE lower(s.schema_name) = ? AND lower(obj.{kind}_name) = ?
                  AND obj.begin_snapshot <= ? AND (obj.end_snapshot IS NULL OR obj.end_snapshot > ?)
                  AND s.begin_snapshot <= ? AND (s.end_snapshot IS NULL OR s.end_snapshot > ?)
            """, [schema, name, snapshot_id, snapshot_id, snapshot_id, snapshot_id]).fetchall()
            if len(rows) != 1:
                raise ValueError("source relation missing or ambiguous at the DuckLake snapshot")
            object_id, object_uuid, *view = rows[0]
            if kind == "view":
                dialect, definition, aliases = view
                checked = checked_select(definition, allowed_relations, aggregate=False) if dialect == "duckdb" and not aliases else None
                if not checked:
                    raise ValueError("view definition is outside the approved lineage subset")
                for parent in checked[1]:
                    resolve(parent, ".".join(relation))
            visiting.remove(relation)
            resolved[relation] = (kind, object_id, object_uuid)
        edges.add(Edge(dependent, ".".join(relation), *resolved[relation]))

    for root in roots:
        resolve(root, name)
    return edges


def changes_affect(history, anchor, snapshot, edges):
    """Return true if changes touch this graph or the history cannot prove otherwise."""
    expected = snapshot[0] - anchor[0]
    if not 0 <= expected <= MAX_HISTORY_SNAPSHOTS or len(history) != expected + 1:
        return True
    if history[0][:3] != anchor or history[-1][:3] != snapshot:
        return True
    table_ids = {str(edge.source_id) for edge in edges if edge.source_kind == "table"}
    view_ids = {str(edge.source_id) for edge in edges if edge.source_kind == "view"}
    for offset, (snapshot_id, _, _, changes) in enumerate(history[1:], 1):
        if snapshot_id != anchor[0] + offset or not isinstance(changes, dict) or not changes:
            return True
        for kind, values in changes.items():
            if not isinstance(values, list) or not values or not all(isinstance(value, str) for value in values):
                return True
            if kind in NAMESPACE_CHANGES:
                # Names, object types and UUIDs were already resolved at both endpoints.
                continue
            touched = table_ids if kind in TABLE_CHANGES else view_ids if kind in VIEW_CHANGES else None
            if touched is None or not all(value.isascii() and value.isdecimal() for value in values):
                return True
            if not touched.isdisjoint(values):
                return True
    return False
