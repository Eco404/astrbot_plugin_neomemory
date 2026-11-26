"""Explicit cleanup operations for one memory space."""

from __future__ import annotations

from typing import Any, Iterable

try:
    from ..storage.job_store import JobConflict
    from ..storage.source_scan import SourceScanner
except ImportError:
    from storage.job_store import JobConflict
    from storage.source_scan import SourceScanner


PROJECTS = frozenset({"sources", "long_term", "profiles", "relationship", "short_term"})
_LONG_TERM_KINDS = (
    "extract", "rebuild", "reindex", "topic_consolidate", "topic_rebuild",
    "section_rebuild", "profile_rebuild", "relationship_rebuild", "context_rebuild",
)
_PROFILE_KINDS = ("profile_rebuild", "context_rebuild", "section_rebuild")
_RELATIONSHIP_KINDS = ("relationship_rebuild", "context_rebuild", "section_rebuild")
_SHORT_TERM_KINDS = ("short_term",)


def _placeholders(values: Iterable[Any]) -> str:
    return ",".join("?" for _ in values)


class SpaceDataService:
    """Delete selected authoritative and derived data without touching other spaces."""

    def __init__(self, source_store, memory_store, *, ingress_store=None, cache=None, image_cache=None):
        self.sources = source_store
        self.memory = memory_store
        self.ingress = ingress_store
        self.cache = cache
        self.image_cache = image_cache
        self.scanner = SourceScanner(source_store)

    async def _source_ids(self, space) -> list[str]:
        query, parameters = self.scanner._scope_query(space, include_events=True)
        rows = await self.sources.database.fetchall(
            query + "SELECT source_id FROM eligible ORDER BY scan_cursor", parameters,
        )
        return [row["source_id"] for row in rows]

    @staticmethod
    async def _assert_idle(connection, space_id: str) -> None:
        row = await (await connection.execute(
            "SELECT job_id FROM memory_jobs WHERE scope_key=? AND state IN ('queued','running','retry_wait') LIMIT 1",
            (space_id,),
        )).fetchone()
        if row:
            raise JobConflict("space has an active task")

    @staticmethod
    async def _delete_jobs(connection, space_id: str, kinds: Iterable[str] | None = None) -> int:
        values = list(kinds or ())
        clause = "scope_key=?"
        parameters: list[Any] = [space_id]
        if values:
            clause += " AND kind IN (" + _placeholders(values) + ")"
            parameters.extend(values)
        rows = await (await connection.execute(
            "SELECT job_id FROM memory_jobs WHERE " + clause, parameters,
        )).fetchall()
        job_ids = [row["job_id"] for row in rows]
        if not job_ids:
            return 0
        for start in range(0, len(job_ids), 400):
            batch = job_ids[start:start + 400]
            ids = _placeholders(batch)
            # These tables do not cascade from memory_jobs and must be removed first.
            for table in ("rebuild_receipts", "rebuild_runs", "short_term_receipts", "note_receipts",
                          "memory_job_candidates", "topic_rebuild_stage"):
                await connection.execute(f"DELETE FROM {table} WHERE job_id IN ({ids})", batch)
            await connection.execute(f"DELETE FROM memory_jobs WHERE job_id IN ({ids})", batch)
        return len(job_ids)

    async def _clear_memory_projects(self, connection, space_id: str, projects: set[str]) -> dict[str, int]:
        result = {project: 0 for project in PROJECTS}
        generation_rows = await (await connection.execute(
            "SELECT generation_id FROM memory_generations WHERE space_id=?", (space_id,),
        )).fetchall()
        generation_ids = [row["generation_id"] for row in generation_rows]

        if "long_term" in projects and generation_ids:
            node_rows = await (await connection.execute(
                "SELECT node_id FROM memory_nodes WHERE space_id=?", (space_id,),
            )).fetchall()
            node_ids = [row["node_id"] for row in node_rows]
            for start in range(0, len(node_ids), 400):
                batch = node_ids[start:start + 400]
                node_values = _placeholders(batch)
                await connection.execute(f"DELETE FROM recall_reinforcements WHERE node_id IN ({node_values})", batch)
                await connection.execute(
                    f"DELETE FROM topic_dependencies WHERE topic_id IN ({node_values}) OR member_id IN ({node_values})",
                    batch * 2,
                )
                await connection.execute(f"DELETE FROM topic_reviews WHERE node_id IN ({node_values})", batch)
                await connection.execute(f"DELETE FROM memory_nodes WHERE node_id IN ({node_values})", batch)
            if node_ids:
                result["long_term"] = len(node_ids)
            batch_rows = await (await connection.execute(
                "SELECT batch_id FROM extraction_batches WHERE space_id=?", (space_id,),
            )).fetchall()
            batch_ids = [row["batch_id"] for row in batch_rows]
            for start in range(0, len(batch_ids), 400):
                batch = batch_ids[start:start + 400]
                values = _placeholders(batch)
                await connection.execute(f"DELETE FROM pending_image_evidence WHERE batch_id IN ({values})", batch)
            await connection.execute("DELETE FROM extraction_batches WHERE space_id=?", (space_id,))
            for start in range(0, len(generation_ids), 400):
                batch = generation_ids[start:start + 400]
                values = _placeholders(batch)
                await connection.execute(f"DELETE FROM topic_receipts WHERE generation_id IN ({values})", batch)
                await connection.execute(f"DELETE FROM derived_cursors WHERE generation_id IN ({values})", batch)
            await self._delete_jobs(connection, space_id, _LONG_TERM_KINDS)

        if "profiles" in projects:
            cursor = await connection.execute("DELETE FROM user_profiles WHERE space_id=?", (space_id,))
            result["profiles"] = cursor.rowcount
            await self._delete_jobs(connection, space_id, _PROFILE_KINDS)

        if "relationship" in projects:
            cursor = await connection.execute("DELETE FROM relationship_states WHERE space_id=?", (space_id,))
            result["relationship"] = cursor.rowcount
            await self._delete_jobs(connection, space_id, _RELATIONSHIP_KINDS)

        if "short_term" in projects:
            cursor = await connection.execute("DELETE FROM short_term_states WHERE space_id=?", (space_id,))
            result["short_term"] = cursor.rowcount
            await self._delete_jobs(connection, space_id, _SHORT_TERM_KINDS)

        return result

    async def _clear_sources_in_memory(self, connection, source_ids: list[str]) -> None:
        if not source_ids:
            return
        for start in range(0, len(source_ids), 400):
            batch = source_ids[start:start + 400]
            values = _placeholders(batch)
            await connection.execute(f"DELETE FROM node_sources WHERE source_id IN ({values})", batch)
            await connection.execute(f"DELETE FROM memory_derivation_sources WHERE source_id IN ({values})", batch)
            await connection.execute(f"DELETE FROM memory_derivation_context_sources WHERE source_id IN ({values})", batch)
            await connection.execute(f"DELETE FROM extraction_batch_sources WHERE source_id IN ({values})", batch)

    async def _delete_sources(self, source_ids: list[str]) -> None:
        if not source_ids:
            return
        if self.ingress is not None:
            await self.ingress.delete_source_ids(source_ids)
        async with self.sources.database.transaction() as connection:
            for start in range(0, len(source_ids), 400):
                batch = source_ids[start:start + 400]
                values = _placeholders(batch)
                await connection.execute(f"DELETE FROM source_records WHERE source_id IN ({values})", batch)
            await connection.execute(
                "DELETE FROM source_cursors WHERE stream_id IN "
                "(SELECT c.stream_id FROM source_cursors c LEFT JOIN source_records r USING(stream_id) "
                "WHERE r.source_id IS NULL)",
            )
            await connection.execute(
                "DELETE FROM conversation_streams WHERE stream_id NOT IN "
                "(SELECT DISTINCT stream_id FROM source_records)",
            )

    async def clear(self, space: dict[str, Any], projects: Iterable[str], *, destroy: bool = False) -> dict[str, Any]:
        space_id = space["space_id"]
        selected = set(projects)
        if destroy:
            selected = set(PROJECTS)
        if not selected or not selected <= PROJECTS:
            raise ValueError("invalid cleanup projects")
        source_ids = await self._source_ids(space) if ("sources" in selected or "long_term" in selected or destroy) else []
        async with self.memory.database.transaction() as connection:
            await self._assert_idle(connection, space_id)
            if destroy:
                await self._delete_jobs(connection, space_id)
                await connection.execute("DELETE FROM memory_spaces WHERE space_id=?", (space_id,))
                result = {project: True for project in PROJECTS}
            else:
                result = await self._clear_memory_projects(connection, space_id, selected)
                if "sources" in selected:
                    await self._clear_sources_in_memory(connection, source_ids)
        if "sources" in selected:
            await self._delete_sources(source_ids)
            if not destroy:
                result["sources"] = len(source_ids)
        if self.image_cache is not None and (destroy or "sources" in selected or "long_term" in selected):
            await self.image_cache.discard_many(source_ids)
        if self.cache is not None and (destroy or "sources" in selected or "long_term" in selected):
            await self.cache.invalidate_namespace(space_id)
        if destroy:
            return {"deleted": True, "source_count": len(source_ids)}
        return {"deleted": False, "source_count": len(source_ids) if "sources" in selected else 0, "counts": result}
