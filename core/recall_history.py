"""Optional persistent snapshots of completed recall requests."""

import json
import logging
import sqlite3
import time
import uuid

from .memory_text import render_display_text

logger = logging.getLogger(__name__)
_RECORDING_KEY = "recall_recording_session"


class RecallHistory:
    def __init__(self, database):
        self.database = database
        self._session = None

    async def load(self):
        row = await self.database.fetchone("SELECT value_json FROM pipeline_state WHERE state_key=?", (_RECORDING_KEY,))
        self._session = json.loads(row[0]) if row else None

    @property
    def enabled(self):
        return self._session is not None

    async def set_enabled(self, enabled):
        async with self.database.transaction() as conn:
            if enabled:
                await conn.execute(
                    "INSERT INTO pipeline_state(state_key,value_json,updated_at) VALUES (?,?,?) "
                    "ON CONFLICT(state_key) DO NOTHING",
                    (_RECORDING_KEY, json.dumps(uuid.uuid4().hex), time.time()))
                cursor = await conn.execute("SELECT value_json FROM pipeline_state WHERE state_key=?", (_RECORDING_KEY,))
                session = json.loads((await cursor.fetchone())[0])
            else:
                await conn.execute("DELETE FROM pipeline_state WHERE state_key=?", (_RECORDING_KEY,))
                await conn.execute("DELETE FROM recall_history")
                session = None
        self._session = session

    def ticket(self):
        return self._session

    async def append(self, ticket, space_id, snapshot):
        if ticket is None or ticket != self._session:
            return
        try:
            async with self.database.transaction() as conn:
                # A late completion can outlive a disable/re-enable or a plugin reload.
                await conn.execute(
                    "INSERT INTO recall_history(space_id,snapshot_json) "
                    "SELECT ?,? FROM pipeline_state WHERE state_key=? AND value_json=?",
                    (space_id, json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
                     _RECORDING_KEY, json.dumps(ticket)))
                await conn.execute(
                    "DELETE FROM recall_history WHERE id IN "
                    "(SELECT id FROM recall_history ORDER BY id DESC LIMIT -1 OFFSET 50)")
        except (sqlite3.Error, RuntimeError) as exc:
            # Recording is optional. A storage failure must not discard the recall result.
            logger.warning("NeoMemory recall recording unavailable (%s)", type(exc).__name__)

    async def list(self, space_id):
        rows = await self.database.fetchall(
            "SELECT snapshot_json FROM recall_history WHERE space_id=? ORDER BY id DESC", (space_id,))
        return [json.loads(row[0]) for row in rows]


def recall_snapshot(mode, query, started, result, *, text=None, hits=None, reason=None):
    diagnostics = result.diagnostics
    items = [
        {"node_id": hit["node_id"], "node_type": hit["node_type"], "score": hit.get("score"),
         "title": render_display_text(hit.get("title") or "", result.user_names),
         **({"source_scene": hit["source_scene"], "source_space_id": hit["external_space_id"]}
            if "external_space_id" in hit else {}),
         **({"similarity": hit["similarity"]} if "similarity" in hit else {})}
        for hit in (result.hits if hits is None else hits)
    ]
    rendered = result.text if text is None else text
    if reason is None:
        if diagnostics.get("deadline_exceeded"):
            reason = "召回达到时间上限"
        elif diagnostics.get("embedding_degraded") or diagnostics.get("active_recall_degraded"):
            reason = "向量检索暂时不可用"
        elif diagnostics.get("embedding_changed"):
            reason = "向量模型设置已切换，请重新查询"
        elif diagnostics.get("cache_degraded"):
            reason = "向量缓存读取异常"
        elif diagnostics.get("missing_node_vectors"):
            reason = f"有 {diagnostics['missing_node_vectors']} 条记忆缺少向量"
        elif not items and not rendered:
            reason = "没有可返回的记忆"
    snapshot = {"mode": mode, "query": query[:4000], "created_at": time.time(),
                "elapsed_ms": round((time.monotonic() - started) * 1000),
                "text": rendered, "items": items, "reason": reason}
    if "missing_node_vectors" in diagnostics:
        total = diagnostics["snapshot_nodes"]
        snapshot["index_coverage"] = {"total": total, "usable": total - diagnostics["missing_node_vectors"],
                                      "truncated": diagnostics["truncated"]}
    if "cache_hit_count" in (embedding := diagnostics.get("query_embedding", {})):
        snapshot["query_vectors"] = {"total": diagnostics["query_count"], "reused": embedding["cache_hit_count"],
                                     "provider_calls": embedding["provider_call_count"]}
    if mode == "test":
        snapshot["preview_candidates"] = [
            {**row, "title": render_display_text(row.get("title") or "", result.user_names)}
            for row in diagnostics.get("preview_candidates", ())
        ]
        for key in ("similarity_threshold", "min_strength"):
            if key in diagnostics:
                snapshot[key] = diagnostics[key]
    return snapshot
