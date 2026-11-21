"""Opt-in stage-five smoke: real providers, automatic extraction, isolated rebuild.

Only synthetic conversation/persona inputs are sent. The script uses temporary
source/memory/cache databases and prints safe aggregate evidence, never provider
logs, credentials, endpoints, source bodies, model responses or exception text.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import io
import json
import logging
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

try:
    from .verify_stage4_api import credentials, failure
except ImportError:
    from verify_stage4_api import credentials, failure


async def wait_terminal(engine, *, kind: str, job_id: str | None = None, timeout: float = 150) -> tuple[dict, list[str]]:
    """Observe the real background runner; no handler is invoked by the probe."""
    seen = []
    async with asyncio.timeout(timeout):
        while True:
            jobs = await engine.jobs.list(limit=20)
            matches = [job for job in jobs if job["kind"] == kind and (job_id is None or job["job_id"] == job_id)]
            if matches:
                job = matches[0]
                marker = f"{job['state']}:{job['step']}"
                if not seen or seen[-1] != marker:
                    seen.append(marker)
                if job["state"] not in {"queued", "running", "retry_wait"}:
                    return job, seen
            await asyncio.sleep(0.1)


def safe_job(job: dict) -> dict:
    return {key: job.get(key) for key in (
        "state", "step", "attempt", "max_attempts", "error_type", "error_code", "progress_done", "progress_total",
    )}


async def run_checks(secrets: dict, directory: Path, proxy: str) -> dict:
    from astrbot import __version__ as host_version
    from astrbot.core.provider.sources.openai_embedding_source import (
        OpenAIEmbeddingProvider,
    )
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
    from astrbot.core.star.context import Context

    from adapters.runtime import MemoryRuntime
    from core.config import NeoMemoryConfig
    from core.identity import EventIdentity
    from core.maintenance import MaintenanceEngine
    from core.source_ingest import CapturedSource
    from storage.memory_store import MemoryStore
    from storage.source_store import SourceStore

    llm_config, embedding_config = secrets["ecoapi"], secrets["Cloudflare Embedding"]
    llm = ProviderOpenAIOfficial({
        "id": "isolated-llm", "type": "openai_chat_completion", "model": llm_config["model"],
        "api_base": llm_config["base_url"], "key": [llm_config["api_token"]], "timeout": 60,
    }, {})
    embedding = OpenAIEmbeddingProvider({
        "id": "isolated-embedding", "type": "openai_embedding", "embedding_model": embedding_config["model"],
        "embedding_api_base": embedding_config["api_address"].rstrip("/") + "/accounts/" + embedding_config["account_id"] + "/ai/v1",
        "embedding_api_key": embedding_config["api_token"], "embedding_dimensions": 0,
        "embedding_dimensions_mode": "never", "timeout": 20, "proxy": proxy,
    }, {})
    counts = {"llm": 0, "embedding": 0}

    async def count_llm(_request):
        counts["llm"] += 1

    async def count_embedding(_request):
        counts["embedding"] += 1

    llm.client._client.event_hooks["request"].append(count_llm)
    embedding.client._client.event_hooks["request"].append(count_embedding)
    providers = {"isolated-llm": llm, "isolated-embedding": embedding}

    async def get_provider_by_id(provider_id):
        return providers.get(provider_id)

    context = object.__new__(Context)
    context.provider_manager = SimpleNamespace(inst_map=providers, provider_insts=[llm],
        embedding_provider_insts=[embedding], get_provider_by_id=get_provider_by_id)
    config = NeoMemoryConfig(llm_provider_id="isolated-llm", embedding_provider_id="isolated-embedding",
        automatic_extraction_enabled=True, private_min_messages=5, private_topic_messages=5,
        private_max_messages=5, job_concurrency=1, job_poll_seconds=0.05,
        extraction_scan_seconds=1, extraction_timeout_seconds=90, index_timeout_seconds=60,
        job_max_attempts=1, embedding_batch_size=16)
    sources = SourceStore(directory / "source.db")
    memory = MemoryStore(directory / "memory.db", source_exists=sources.has_source)
    plugin = SimpleNamespace(context=context, config=config, data_dir=directory, source_store=sources,
        memory_store=memory, maintenance=None, ready=True)
    runtime = MemoryRuntime(plugin)

    async def persona_loader(persona_id):
        if persona_id != "synthetic_persona":
            return None
        return "合成测试人格：温和的陪伴者，关心用户持续学习与重要经历，只记录有来源支持的内容。"

    engine = MaintenanceEngine(sources, memory, runtime, config, persona_loader)
    plugin.maintenance = engine
    result = {
        "tested_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
        "host_version": host_version, "host_source": importlib.import_module("astrbot").__file__,
        "llm_model": llm_config["model"], "embedding_model": embedding_config["model"],
        "host_llm_class": type(llm).__name__, "host_embedding_class": type(embedding).__name__,
        "proxy_enabled_for_embedding": bool(proxy), "plugin_max_attempts": 1, "checks": [],
    }
    try:
        await sources.open()
        await memory.open()
        await runtime.start()
        synthetic = [
            "我计划报名为期半年的摄影课程，每周六上午上课。",
            "今天已经缴费完成报名，开课日期是2026年11月7日。",
            "第一次课需要自带相机，我打算先练习光圈和快门。",
            "这次学摄影是为了给家人拍旅行照片，我会坚持上完课程。",
            "你可以记住这件事，之后关心一下我的学习进展。",
        ]
        captured = []
        for index, text in enumerate(synthetic):
            message_id = f"synthetic-{index}"
            identity = EventIdentity(platform_instance_id="isolated-platform", platform_name="isolated",
                message_type="FriendMessage", unified_msg_origin="isolated:FriendMessage:synthetic",
                session_id="synthetic", group_id="", account_id="synthetic-user", account_name="Synthetic",
                self_id="synthetic-agent", message_id=message_id)
            captured.append(await sources.capture(CapturedSource(identity=identity, actor_kind="user",
                content_text=text, observed_at=time.time() - 100 + index, message_id=message_id,
                actor_account_id="synthetic-user", persona_id="synthetic_persona")))
        source_ids = [item.source_id for item in captured]
        started = time.perf_counter()
        before = dict(counts)
        await engine.start()
        automatic, observed = await wait_terminal(engine, kind="extract")
        spaces = await memory.database.fetchall("SELECT * FROM memory_spaces")
        space = dict(spaces[0]) if len(spaces) == 1 else None
        generation = await memory.get_active_generation(space["space_id"]) if space else None
        nodes = await memory.list_nodes(space_id=space["space_id"]) if space else []
        consumed = await engine.pipeline.consumed(generation["generation_id"], source_ids) if generation else set()
        assertions = {
            "job_succeeded": automatic["state"] == "succeeded",
            "automatic_scanner_request": automatic["payload"]["request"] == {"automatic": True},
            "single_space_discovered": len(spaces) == 1,
            "nonempty_memories": bool(nodes), "all_sources_consumed": consumed == set(source_ids),
            "activity_round_once": bool(generation and generation["activity_round"] == 1),
            "source_database_unchanged": (await sources.health())["source_count"] == len(source_ids),
        }
        result["checks"].append({"name": "automatic_extraction", "ok": all(assertions.values()),
            "seconds": round(time.perf_counter() - started, 4), "http_requests": {key: counts[key] - before[key] for key in counts},
            "source_count": len(source_ids), "node_count": len(nodes), "node_types": sorted({node["node_type"] for node in nodes}),
            "job": safe_job(automatic), "observed_states_steps": observed, "assertions": assertions})
        if not all(assertions.values()):
            result["checks"].append({"name": "new_generation_rebuild", "ok": False, "reason": "prerequisite_failed"})
            return result
        original_generation = generation["generation_id"]
        original_ids = {node["node_id"] for node in nodes}
        started = time.perf_counter()
        before = dict(counts)
        queued = await engine.enqueue("rebuild", space["space_id"], "isolated-stage5-rebuild")
        rebuilt, observed = await wait_terminal(engine, kind="rebuild", job_id=queued["job_id"])
        active = await memory.get_active_generation(space["space_id"])
        new_nodes = await memory.list_nodes(space_id=space["space_id"])
        old = await memory.database.fetchone("SELECT status FROM memory_generations WHERE generation_id=?", (original_generation,))
        consumed = await engine.pipeline.consumed(active["generation_id"], source_ids)
        receipt = await engine.pipeline.get_rebuild_receipt(rebuilt["job_id"])
        embeddings = runtime.recall.embeddings
        vectors = await embeddings.cached([node["content_text"] for node in new_nodes], namespace=space["space_id"],
            kind="node", identities=[(node["node_id"], node["content_version"]) for node in new_nodes])
        assertions = {
            "job_succeeded": rebuilt["state"] == "succeeded",
            "new_generation_active": active["generation_id"] != original_generation,
            "old_generation_retired": bool(old and old["status"] == "retired"),
            "nonempty_memories": bool(new_nodes),
            "node_identities_rebuilt": bool(new_nodes) and original_ids.isdisjoint(node["node_id"] for node in new_nodes),
            "all_sources_replayed": consumed == set(source_ids),
            "frozen_source_count": queued["payload"]["total"] == len(source_ids),
            "source_database_unchanged": (await sources.health())["source_count"] == len(source_ids),
            "activity_round_once": active["activity_round"] == 1,
            "rebuild_receipt_exists": receipt is not None,
            "new_node_vectors_cached": bool(vectors) and all(vector is not None for vector in vectors),
            "progress_complete": rebuilt["progress_done"] == rebuilt["progress_total"] == len(source_ids),
        }
        result["checks"].append({"name": "new_generation_rebuild", "ok": all(assertions.values()),
            "seconds": round(time.perf_counter() - started, 4), "http_requests": {key: counts[key] - before[key] for key in counts},
            "node_count": len(new_nodes), "node_types": sorted({node["node_type"] for node in new_nodes}),
            "embedding_dimensions": sorted({len(vector) for vector in vectors if vector is not None}),
            "job": safe_job(rebuilt), "observed_states_steps": observed, "assertions": assertions})
    finally:
        result["http_requests_total"] = counts
        await engine.stop()
        await runtime.close()
        await memory.close()
        await sources.close()
        await llm.terminate()
        await embedding.terminate()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    parser.add_argument("--embedding-proxy", default="")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        with tempfile.TemporaryDirectory(prefix="neomemory-stage5-api-") as temporary:
            directory = Path(temporary)
            (directory / "data/config").mkdir(parents=True)
            os.environ["ASTRBOT_ROOT"] = str(directory)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                from loguru import logger
                logger.remove()
                result = asyncio.run(run_checks(credentials(args.credentials), directory, args.embedding_proxy))
    except Exception as exc:
        result = {"setup_or_run": failure(exc)}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    checks = result.get("checks", [])
    return 0 if len(checks) == 2 and all(check["ok"] for check in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
