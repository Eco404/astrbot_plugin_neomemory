"""抽取候选与提交后索引的有界读取，不复用召回强度筛选。"""

from __future__ import annotations

from .recall_store import _NODE_SELECT


class ExtractionContextStoreMixin:
    async def extraction_context_page(
        self, space_id, generation_id, *, after="", limit=128, recent=False,
    ):
        """按稳定节点ID遍历当前构建代；近期读取只用于保留有限上下文。"""
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("context page size must be between 1 and 256")
        async with self.database.transaction(write=False) as connection:
            cursor = await connection.execute(
                "SELECT 1 FROM memory_generations WHERE space_id=? AND generation_id=? "
                "AND status IN ('active','building')", (space_id, generation_id),
            )
            if await cursor.fetchone() is None:
                raise ValueError("context generation is absent or outside this space")
            conditions = (
                "n.space_id=? AND n.generation_id=? AND n.status='active' "
                "AND COALESCE(json_extract(n.metadata_json,'$.summary_stale'),0)=0"
            )
            values = [space_id, generation_id]
            if not recent:
                conditions += " AND n.node_id>?"
                values.append(after)
            order = "n.updated_at DESC,n.node_id" if recent else "n.node_id"
            cursor = await connection.execute(
                _NODE_SELECT + f" WHERE {conditions} ORDER BY {order} LIMIT ?",
                (*values, limit + 1),
            )
            rows = await cursor.fetchall()
        nodes = [self._node_from_row(row) for row in rows[:limit]]
        return {"nodes": nodes, "has_more": len(rows) > limit,
                "after": nodes[-1]["node_id"] if nodes else after}

    async def extraction_batch_index_nodes(self, batch_id):
        """仅返回该成功批次新增/修改的有效正文，标题操作不触发向量准备。"""
        async with self.database.transaction(write=False) as connection:
            cursor = await connection.execute(
                "SELECT b.space_id,b.generation_id,g.status FROM extraction_batches b "
                "JOIN memory_generations g ON b.generation_id=g.generation_id WHERE b.batch_id=?",
                (batch_id,),
            )
            batch = await cursor.fetchone()
            if batch is None or batch["status"] not in {"active", "building"}:
                raise ValueError("index batch is absent or its generation is not usable")
            cursor = await connection.execute(
                _NODE_SELECT + " JOIN memory_derivations d ON d.node_version_id=v.node_version_id "
                "WHERE d.batch_id=? AND d.operation IN ('create','update') "
                "AND n.space_id=? AND n.generation_id=? AND n.status IN ('active','historical') "
                "AND COALESCE(json_extract(n.metadata_json,'$.summary_stale'),0)=0 "
                "ORDER BY n.node_id",
                (batch_id, batch["space_id"], batch["generation_id"]),
            )
            rows = await cursor.fetchall()
        return [self._node_from_row(row) for row in rows]
