"""后台抽取的近期及语义候选；分页扫描全空间，模型上下文保持有界。"""

from __future__ import annotations

import asyncio
import math
import time

import numpy as np

CONTEXT_VERSION = "extraction-context.v1"
CONTEXT_PAGE_SIZE = 128
QUERY_LIMIT = 10
QUERY_TEXT_CHARS = 4000
QUERY_COVERAGE_MIN_SCORE = 0.35


def window_query_texts(sources):
    """均匀覆盖窗口首尾及中间，使用与消息缓存相同的原文片段。"""
    texts = [source["content_text"][:QUERY_TEXT_CHARS] for source in sources if source["content_text"].strip()]
    if len(texts) > QUERY_LIMIT:
        indices = [round(index * (len(texts) - 1) / (QUERY_LIMIT - 1)) for index in range(QUERY_LIMIT)]
        texts = [texts[index] for index in indices]
    return list(dict.fromkeys(texts))


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("extraction context deadline exceeded")
    return remaining


def _rank_page_with_query_coverage(best, query_best, nodes, vectors, queries, limit):
    """Keep global ranking and one above-threshold representative per query.

    The per-query candidates are only a coverage aid. Below-threshold
    scores do not reserve a representative; the threshold does not prove
    semantic relevance.  Scores and IDs are retained instead of node objects so pages
    can be processed incrementally with deterministic tie breaking.
    """
    matrix = np.asarray(vectors, dtype=float) @ np.asarray(queries, dtype=float).T
    scores = np.max(matrix, axis=1)
    candidates = best + [(float(score), node["node_id"]) for node, score in zip(nodes, scores, strict=True)]
    best[:] = sorted(candidates, key=lambda item: (-item[0], item[1]))[:limit]
    for query_index in range(matrix.shape[1]):
        query_scores = matrix[:, query_index]
        score = float(np.max(query_scores))
        if score < QUERY_COVERAGE_MIN_SCORE:
            continue
        node_id = min(nodes[index]["node_id"] for index in np.flatnonzero(query_scores == score))
        previous = query_best.get(query_index)
        candidate = (score, node_id)
        if previous is None or (-score, node_id) < (-previous[0], previous[1]):
            query_best[query_index] = candidate


async def node_vectors(embeddings, nodes, space_id, *, deadline, require_persisted=False):
    """复用已有版本向量，只为缺项计算；离线索引需读回确认而非假定写入成功。"""
    async with asyncio.timeout(_remaining(deadline)):
        texts = [node["content_text"] for node in nodes]
        identities = [(node["node_id"], node["content_version"]) for node in nodes]
        vectors = await embeddings.cached(
            texts, namespace=space_id, kind="node", identities=identities,
            allow_text_reuse=not require_persisted,
        )
        missing = [index for index, vector in enumerate(vectors) if vector is None]
        batch_size = embeddings.batch_size
        for start in range(0, len(missing), batch_size):
            positions = missing[start:start + batch_size]
            calculated = await embeddings.embed(
                [texts[index] for index in positions], namespace=space_id, kind="node",
                identities=[identities[index] for index in positions], timeout_seconds=_remaining(deadline),
                require_persisted=require_persisted,
            )
            for index, vector in zip(positions, calculated, strict=True):
                vectors[index] = vector
            if require_persisted:
                stored = await embeddings.cached(
                    [texts[index] for index in positions], namespace=space_id, kind="node",
                    identities=[identities[index] for index in positions], allow_text_reuse=False,
                )
                if any(vector is None for vector in stored):
                    raise IndexCacheUnavailable()
            await asyncio.sleep(0)
        return vectors, len(missing)


class IndexCacheUnavailable(RuntimeError):
    """向量已计算但缓存未保留；抽取完成凭据仍然有效。"""


async def select_context_ids(
    memory, embeddings, *, space_id, generation_id, sources, limit, timeout_seconds, progress=None,
):
    if limit == 0:
        return ()
    recent_page = await memory.extraction_context_page(space_id, generation_id, limit=limit, recent=True)
    recent = [node["node_id"] for node in recent_page["nodes"]]
    texts = window_query_texts(sources)
    if embeddings is None or not recent or not texts:
        return tuple(recent)
    deadline = time.monotonic() + timeout_seconds
    fingerprint = embeddings.fingerprint
    async with asyncio.timeout(timeout_seconds):
        queries = await embeddings.embed(texts, namespace=space_id, timeout_seconds=_remaining(deadline))
        best, query_best, after, scanned = [], {}, "", 0
        while True:
            page = await memory.extraction_context_page(
                space_id, generation_id, after=after, limit=CONTEXT_PAGE_SIZE,
            )
            nodes = page["nodes"]
            if not nodes:
                break
            vectors, _ = await node_vectors(embeddings, nodes, space_id, deadline=deadline)
            if embeddings.fingerprint != fingerprint:
                raise ValueError("embedding provider changed during context preparation")
            await asyncio.to_thread(
                _rank_page_with_query_coverage, best, query_best, nodes, vectors, queries, limit,
            )
            scanned += len(nodes)
            if progress is not None:
                await progress(scanned)
            if not page["has_more"]:
                break
            after = page["after"]
    selected = recent[:max(1, math.ceil(limit / 3))]
    covered = [node_id for _, node_id in sorted(query_best.values(), key=lambda item: (-item[0], item[1]))]
    for node_id in covered + [node_id for _, node_id in best] + recent:
        if len(selected) == limit:
            break
        if node_id not in selected:
            selected.append(node_id)
    return tuple(selected)
