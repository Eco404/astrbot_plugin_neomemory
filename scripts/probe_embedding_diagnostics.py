"""Opt-in independent cold/hot recall with real AstrBot embedding calls only.

This measures one isolated query pair, not latency percentiles or recall quality.
No generative model, real conversations, or production database is used.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import logging
import os
from pathlib import Path
import time
from types import SimpleNamespace

from scripts.verify_stage4_api import credentials, failure

NODES = (
    "{{user:synthetic-visitor}}报名了周末的植物标本制作活动，准备携带纸袋和标签。",
    "我和{{user:synthetic-visitor}}约好先查看显微镜的使用说明，再一起练习调焦。",
    "{{user:synthetic-visitor}}正在整理海岸岩石照片，按拍摄地点分类。",
)
CURRENT = "周末的标本活动需要带什么物品？"
HISTORY = (
    ("user", "我还想看看植物的细胞结构。"),
    ("agent", "我们先熟悉显微镜的调焦方法。"),
)


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


async def run(directory, secrets, proxy):
    from astrbot import __version__ as host_version
    from astrbot.core.provider.sources.openai_embedding_source import OpenAIEmbeddingProvider
    from astrbot.core.star.context import Context
    from adapters.providers import ProviderAdapter
    from core.embeddings import EmbeddingService
    from core.memory_spaces import private_space
    from core.retrieval import RecallService
    from storage.cache_store import CacheStore
    from storage.memory_store import MemoryStore

    cfg = secrets["Cloudflare Embedding"]
    provider = OpenAIEmbeddingProvider({"id": "probe-embedding", "type": "openai_embedding",
        "embedding_model": cfg["model"],
        "embedding_api_base": cfg["api_address"].rstrip("/") + "/accounts/" + cfg["account_id"] + "/ai/v1",
        "embedding_api_key": cfg["api_token"], "embedding_dimensions": 0,
        "embedding_dimensions_mode": "never", "timeout": 20, "proxy": proxy}, {})
    provider.client.max_retries = 0
    report = {"synthetic_only": True, "host_version": host_version, "embedding_model": cfg["model"],
              "http_requests": 0, "generative_calls": 0, "checks": [], "sdk_retries": 0,
              "limits": "One cold/hot pair; no accuracy labels or tail-latency claims."}

    async def count(_request):
        if report["http_requests"] >= 8:
            raise RuntimeError("probe_http_limit")
        report["http_requests"] += 1

    provider.client._client.event_hooks["request"].append(count)
    async def lookup(identifier):
        return provider if identifier == "probe-embedding" else None
    host = object.__new__(Context)
    host.provider_manager = SimpleNamespace(inst_map={"probe-embedding": provider}, provider_insts=[],
        embedding_provider_insts=[provider], get_provider_by_id=lookup)
    cache = CacheStore(directory / "cache.db")
    memory = MemoryStore(directory / "memory.db")
    await cache.open()
    await memory.open()
    embedding = EmbeddingService(ProviderAdapter(host), cache, provider_id="probe-embedding", batch_size=2)
    try:
        space = await memory.get_or_create_space(private_space("synthetic-persona", "synthetic-visitor"))
        for index, content in enumerate(NODES):
            await memory.create_node(space_id=space["space_id"], node_type="event", content_text=content,
                source_ids=[f"synthetic-source-{index}"], importance=.6)
        recall = RecallService(memory, embedding)
        await recall.index_space(space["space_id"], timeout_seconds=30)
        history_diagnostics = {}
        await embedding.embed([text for _, text in HISTORY], namespace=space["space_id"],
                              timeout_seconds=30, diagnostics=history_diagnostics)
        report["history_preparation"] = history_diagnostics
        for label in ("cold_current_cached_history", "hot_current_cached_history"):
            before = report["http_requests"]
            now = time.time()
            history = [{"actor_kind": role, "content_text": text, "observed_at": now - 10 + index}
                       for index, (role, text) in enumerate(HISTORY)]
            result = await recall.recall(space["space_id"], CURRENT, history=history, now=now,
                                         user_names={"synthetic-visitor": "访客"})
            diagnostic = result.diagnostics
            expected_cached = 2 if label.startswith("cold") else 3
            check = {"case": label, "http_requests": report["http_requests"] - before,
                "hit_count": len(result.hits), "rendered_utf8_bytes": len(result.text.encode("utf-8")),
                "diagnostics": diagnostic}
            check["ok"] = (diagnostic.get("query_count") == diagnostic.get("usable_query_count") == 3
                and diagnostic.get("cached_query_count") == expected_cached and diagnostic.get("current_query_available")
                and not diagnostic.get("embedding_degraded") and not diagnostic.get("deadline_exceeded")
                and check["http_requests"] == (1 if label.startswith("cold") else 0))
            report["checks"].append(check)
            save(directory / "report.json", report)
    except Exception as exc:
        report["failure"] = failure(exc)
    finally:
        await memory.close()
        await cache.close()
        await provider.terminate()
    save(directory / "report.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    parser.add_argument("--proxy", default="http://192.168.5.100:7890")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "host/data/config").mkdir(parents=True)
    os.environ["ASTRBOT_ROOT"] = str((args.output / "host").resolve())
    save(args.output / "manifest.json", {"nodes": NODES, "current": CURRENT, "history": HISTORY,
        "cases": ["cold_current_cached_history", "hot_current_cached_history"], "http_limit": 8,
        "expected": "Each recall uses 3 vectors; first current query makes 1 HTTP call, hot repeat makes 0.",
        "generative_calls": 0, "sdk_retries": 0})
    logging.disable(logging.CRITICAL)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from loguru import logger
        logger.remove()
        result = asyncio.run(run(args.output, credentials(args.credentials), args.proxy))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if len(result["checks"]) == 2 and all(item["ok"] for item in result["checks"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
