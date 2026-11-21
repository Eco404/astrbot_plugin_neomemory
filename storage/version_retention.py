"""Bound per-entity content history while keeping the current version number."""

import json

VERSION_TABLES = {
    "node": ("memory_node_versions", "node_id"),
    "profile": ("profile_versions", "profile_id"),
    "relationship": ("relationship_versions", "relationship_id"),
}

# Snapshot insertion is the final content write for profile and relationship versions.
# Nodes need their old version until source-clock inheritance completes.
def _trim_entity(kind, entity):
    table, key = VERSION_TABLES[kind]
    return (f"DELETE FROM {table} WHERE {key}={entity} AND version_no < "
            f"(SELECT version_no FROM {table} WHERE {key}={entity} ORDER BY version_no DESC "
            f"LIMIT 1 OFFSET (SELECT keep_count-1 FROM memory_version_limits WHERE kind='{kind}'))")


VERSION_RETENTION_SCHEMA = (
    "CREATE TABLE memory_version_limits(kind TEXT PRIMARY KEY, keep_count INTEGER NOT NULL CHECK(keep_count>=1))",
    "INSERT INTO memory_version_limits VALUES ('node',10),('profile',10),('relationship',10)",
    *(f"CREATE TRIGGER trim_{table} AFTER INSERT ON {table} WHEN NEW.version_no>1 BEGIN "
      + _trim_entity(kind, "NEW." + key) + "; END"
      for kind, (table, key) in VERSION_TABLES.items() if kind != "node"),
)


async def prune_node_history(connection, node_id):
    await connection.execute(_trim_entity("node", "?1"), (node_id,))


async def apply_version_limits(database, limits=None):
    """Prune on startup and configuration changes within a single write transaction."""
    async with database.transaction() as connection:
        if limits is not None:
            await connection.executemany("UPDATE memory_version_limits SET keep_count=? WHERE kind=?",
                                         [(limits[kind], kind) for kind in VERSION_TABLES])
        for kind, (table, key) in VERSION_TABLES.items():
            await connection.execute(
                f"DELETE FROM {table} WHERE rowid IN (SELECT id FROM (SELECT rowid AS id, "
                f"row_number() OVER (PARTITION BY {key} ORDER BY version_no DESC) AS position "
                f"FROM {table}) WHERE position>(SELECT keep_count FROM memory_version_limits WHERE kind=?))",
                (kind,))


async def mark_missing_version_references(database, versions):
    """Mark unavailable baselines without changing their recorded provenance."""
    refs = []
    for version in versions:
        for entry in version["derivations"]:
            refs.extend(entry["base_versions"])
            refs.extend(entry["semantic"].get("member_versions", ()))
    identities = {(r["kind"], r["entity_id"], r["version_no"]) for r in refs}
    if not identities:
        return
    present = set()
    async with database.transaction(write=False) as connection:
        for kind, (table, key) in VERSION_TABLES.items():
            pairs = [(entity, number) for category, entity, number in identities if category == kind]
            if pairs:
                cursor = await connection.execute(
                    f"SELECT {key},version_no FROM json_each(?) q JOIN {table} v "
                    f"ON v.{key}=json_extract(q.value,'$[0]') AND v.version_no=json_extract(q.value,'$[1]')",
                    (json.dumps(pairs),))
                present.update((kind, row[0], row[1]) for row in await cursor.fetchall())
    for ref in refs:
        if (ref["kind"], ref["entity_id"], ref["version_no"]) not in present:
            ref["available"] = False
