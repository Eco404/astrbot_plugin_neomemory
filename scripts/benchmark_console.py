#!/usr/bin/env python3
"""Offline bounded-scale probe; synthetic vectors measure costs, never relevance.

Uses production stores/services in a temporary directory. It does not read plugin
configuration, API keys, user conversations, or call a network provider. Defaults
cross the existing 2,000-node snapshot boundary without changing that boundary.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import json
import math
import os
import platform
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import core.embeddings as embeddings_module  # noqa: E402
import core.retrieval as retrieval_module  # noqa: E402
import storage.cache_store as cache_module  # noqa: E402
from adapters.providers import ProviderAdapter, normalize_vectors  # noqa: E402
from core.embeddings import EmbeddingService  # noqa: E402
from core.memory_spaces import private_space  # noqa: E402
from core.retrieval import RecallPolicy, RecallService  # noqa: E402
from storage.cache_store import CacheStore  # noqa: E402
from storage.console_store import ConsoleStore  # noqa: E402
from storage.memory_store import MemoryStore  # noqa: E402


class SyntheticEmbeddingProvider:
    """Deterministic hash-seeded noise, deliberately without semantic meaning."""

    provider_config = {"id": "offline-synthetic", "model": "hash-seeded-noise-v1"}

    def __init__(self, dimension):
        self.dimension = dimension
        self.calls = 0
        self.texts = 0

    def get_dim(self):
        return self.dimension

    def get_model(self):
        return "hash-seeded-noise-v1"

    async def get_embeddings(self, texts):
        self.calls += 1
        self.texts += len(texts)
        vectors = []
        for text in texts:
            seed = int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")
            vectors.append(np.random.default_rng(seed).standard_normal(self.dimension).tolist())
        await asyncio.sleep(0)
        return vectors


class LocalContext:
    def __init__(self, provider):
        self.provider = provider

    def get_provider_by_id(self, provider_id):
        return self.provider if provider_id == "offline-synthetic" else None

    def get_all_embedding_providers(self):
        return [self.provider]

    def get_all_providers(self):
        return []


def quantiles(samples):
    ordered = sorted(samples)
    return {"n": len(samples), "p50_ms": round(ordered[math.ceil(len(samples) * .5) - 1], 3),
            "p95_ms": round(ordered[math.ceil(len(samples) * .95) - 1], 3),
            "min_ms": round(ordered[0], 3), "max_ms": round(ordered[-1], 3),
            "method": "nearest-rank", "samples_ms": [round(value, 3) for value in samples]}


def read_optional(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def hardware():
    cpu_info = read_optional("/proc/cpuinfo") or ""
    cpu_model = next((line.split(":", 1)[1].strip() for line in cpu_info.splitlines()
                      if line.startswith("model name")), platform.processor())
    meminfo = read_optional("/proc/meminfo") or ""
    total = next((line.split(":", 1)[1].strip() for line in meminfo.splitlines()
                  if line.startswith("MemTotal:")), None)
    return {"cpu_model": cpu_model, "logical_cpus": os.cpu_count(),
            "affinity_cpus": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
            "cpu_cgroup_max": read_optional("/sys/fs/cgroup/cpu.max"),
            "memory_cgroup_max": read_optional("/sys/fs/cgroup/memory.max"),
            "host_mem_total": total, "kernel": platform.release(), "machine": platform.machine(),
            "python": platform.python_version(), "numpy": np.__version__}


def disk_bytes(directory):
    return {path.name: path.stat().st_size for path in sorted(Path(directory).iterdir()) if path.is_file()}


class Profile:
    """Script-only wrappers, never stored in production services or databases.

    Categories overlap (cache total includes decode). Sync helper timings are
    CPU worker wall time; caller timings include queueing/scheduling. Results
    are per-call sums, not attribution of all unmeasured Python/SQLite costs.
    """

    def __init__(self):
        self.originals = []
        self.events = []

    def wrap(self, owner, attribute, label, *, async_method=False):
        original = getattr(owner, attribute)
        if async_method or inspect.iscoroutinefunction(original):
            async def measured(*args, **kwargs):
                before = time.perf_counter()
                try:
                    return await original(*args, **kwargs)
                finally:
                    self.events.append((label, (time.perf_counter() - before) * 1000))
        else:
            def measured(*args, **kwargs):
                before = time.perf_counter()
                try:
                    return original(*args, **kwargs)
                finally:
                    self.events.append((label, (time.perf_counter() - before) * 1000))
        self.originals.append((owner, attribute, original))
        setattr(owner, attribute, measured)

    def install(self, cache):
        for owner, attribute, label in [
            (MemoryStore, "retrieval_snapshot", "snapshot"),
            (MemoryStore, "revalidate_recall", "revalidate"),
            (CacheStore, "get_vectors", "cache_total_including_decode"),
            (cache_module, "_decode_vectors", "cache_decode_validation_worker"),
            (embeddings_module, "_normalize_cached_vectors", "normalization_worker"),
            (retrieval_module, "rank_candidates", "ranking_worker"),
            (retrieval_module, "select_context", "context_selection"),
        ]:
            self.wrap(owner, attribute, label)
        self.wrap(cache.database.connection, "executemany", "cache_sql_touch", async_method=True)

    def collect(self):
        totals = {}
        for label, elapsed in self.events:
            totals[label] = totals.get(label, 0) + elapsed
        self.events.clear()
        return totals

    def restore(self):
        for owner, attribute, original in reversed(self.originals):
            setattr(owner, attribute, original)


def reference_normalization(vectors, dimension):
    """Stage-six normalization retained only for reproducible before/after runs."""
    present = [vector for vector in vectors if vector is not None]
    normalized = iter(normalize_vectors(present, count=len(present), dimension=dimension))
    return [next(normalized) if vector is not None else None for vector in vectors]


async def run(args):
    if args.reference_normalization:
        embeddings_module._normalize_cached_vectors = reference_normalization
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="neomemory-bounded-console-") as directory:
        memory = MemoryStore(Path(directory) / "memory.db", source_exists=lambda source: source == "synthetic-source")
        cache = CacheStore(Path(directory) / "cache.db")
        await memory.open()
        await cache.open()
        try:
            space = await memory.get_or_create_space(private_space("synthetic-persona", "large-user"))
            sid = space["space_id"]
            seed_started = time.perf_counter()
            active_texts = []
            for number in range(args.nodes):
                status = "historical" if number % 4 == 0 else "active"
                text = (f"合成经历编号 {number:05d}。" + ("literal 50%_ " if number % 5 == 0 else "ordinary ")
                        + "这是完全离线生成的规模样本。正文仅用于测量数据库读取、向量缓存解析和筛选开销，不代表真实用户的聊天内容。") * args.text_repeat
                await memory.create_node(space_id=sid, node_type="event", content_text=text, status=status,
                    importance=.6, source_ids=["synthetic-source"])
                if status == "active":
                    active_texts.append(text)
            small_spaces = []
            for number in range(args.spaces):
                item = await memory.get_or_create_space(private_space("synthetic-persona", f"small-user-{number}"))
                small_spaces.append(item["space_id"])
                for index in range(20):
                    await memory.create_node(space_id=item["space_id"], node_type="atomic",
                        content_text=f"小空间 {number} 记录 {index} " + ("literal 50%_" if index % 5 == 0 else "ordinary"),
                        source_ids=["synthetic-source"])
            seed_seconds = time.perf_counter() - seed_started
            snapshot = await memory.retrieval_snapshot(sid, include_historical=True)
            passive_snapshot = await memory.retrieval_snapshot(sid)
            assert snapshot["truncated"] and len(snapshot["nodes"]) == 2000
            assert passive_snapshot["truncated"] and len(passive_snapshot["nodes"]) < 2000
            assert all(node["status"] == "active" for node in passive_snapshot["nodes"])
            passive_ids = {node["node_id"] for node in passive_snapshot["nodes"]}
            assert passive_ids <= {node["node_id"] for node in snapshot["nodes"]}
            provider = SyntheticEmbeddingProvider(args.dimension)
            adapter = ProviderAdapter(LocalContext(provider), embedding_concurrency=2)
            embeddings = EmbeddingService(adapter, cache, provider_id="offline-synthetic", batch_size=16)
            recall = RecallService(memory, embeddings, policy=RecallPolicy())
            query = active_texts[-1]
            cold_ms = []
            for _ in range(args.repetitions):
                before = time.perf_counter()
                result = await recall.recall(sid, query)
                cold_ms.append((time.perf_counter() - before) * 1000)
                assert not result.hits and result.diagnostics["missing_node_vectors"] == len(passive_snapshot["nodes"])
                assert not result.diagnostics.get("deadline_exceeded")
            assert provider.calls == provider.texts == 0
            before = time.perf_counter()
            indexed = await recall.index_space(sid, timeout_seconds=120)
            index_seconds = time.perf_counter() - before
            assert indexed == {"indexed": 2000, "cached": 0, "truncated": True}
            assert provider.texts == 2000
            index_calls = provider.calls
            now = time.time()
            history = [{"content_text": text, "actor_kind": "user" if i % 2 == 0 else "agent",
                        "observed_at": now - i - 1} for i, text in enumerate(active_texts[-11:-1])]
            warm = await recall.recall(sid, query, history=history)
            hot_ms, diagnostics, hit_counts, stage_samples = [], [], [], []
            profiler = Profile()
            profiler.install(cache)
            calls_before_hot = provider.calls
            try:
                for _ in range(args.repetitions):
                    before = time.perf_counter()
                    result = await recall.recall(sid, query, history=history)
                    hot_ms.append((time.perf_counter() - before) * 1000)
                    stage_samples.append(profiler.collect())
                    diagnostics.append(result.diagnostics)
                    hit_counts.append(len(result.hits))
                    assert result.diagnostics.get("missing_node_vectors", 0) == 0
            finally:
                profiler.restore()
            assert provider.calls == calls_before_hot
            concurrent_ms, concurrent_diagnostics, concurrent_hits, loop_lag_ms = [], [], [], []
            monitor_running = True

            async def monitor_loop():
                while monitor_running:
                    before = time.perf_counter()
                    await asyncio.sleep(.01)
                    loop_lag_ms.append(max(0, (time.perf_counter() - before - .01) * 1000))

            async def concurrent_recall():
                before = time.perf_counter()
                recalled = await recall.recall(sid, query, history=history)
                concurrent_ms.append((time.perf_counter() - before) * 1000)
                concurrent_diagnostics.append(recalled.diagnostics)
                concurrent_hits.append(len(recalled.hits))
                assert all(hit["space_id"] == sid for hit in recalled.hits)
                assert len(recalled.text.encode()) <= recall.policy.budget_tokens

            monitor = asyncio.create_task(monitor_loop())
            try:
                for _ in range(args.repetitions):
                    await asyncio.gather(*(concurrent_recall() for _ in range(args.concurrent_requests)))
            finally:
                monitor_running = False
                await monitor
            assert provider.calls == calls_before_hot
            # A completed request after concurrent/time-limited work proves that
            # cancellation did not leave the shared stores/embedding lock stuck.
            recovery = await recall.recall(sid, query, history=history)
            assert not recovery.diagnostics.get("deadline_exceeded")
            assert len(recovery.hits) == len(warm.hits)
            console = ConsoleStore(memory)
            large_read_ms = []
            for _ in range(args.repetitions):
                before = time.perf_counter()
                page = await console.nodes(sid, q="50%_", status="active", limit=20, offset=20)
                large_read_ms.append((time.perf_counter() - before) * 1000)
                assert len(page["items"]) == 20 and page["has_more"]
                assert all("50%_" in node["content_text"] and node["status"] == "active" for node in page["items"])
            small_read_ms = []
            for _ in range(args.repetitions):
                for small in small_spaces:
                    before = time.perf_counter()
                    page = await console.nodes(small, q="50%_", limit=2, offset=2)
                    small_read_ms.append((time.perf_counter() - before) * 1000)
                    assert len(page["items"]) == 2 and not page["has_more"]
                    assert all(node["space_id"] == small for node in page["items"])
            cache_count = await cache.database.fetchone("SELECT COUNT(*) n,SUM(LENGTH(vector_json)) bytes FROM embedding_cache")
            open_files = disk_bytes(directory)
            result = {"measured_at_utc": datetime.now(UTC).isoformat(), "hardware": hardware(),
                "workload": {"large_space_nodes": args.nodes, "large_space_historical": math.ceil(args.nodes / 4),
                    "small_spaces": args.spaces, "nodes_per_small_space": 20, "dimension": args.dimension,
                    "batch_size": 16, "embedding_concurrency": 2, "history_queries": len(history),
                    "repetitions": args.repetitions, "text_repeat": args.text_repeat,
                    "node_chars": len(query), "node_utf8_bytes": len(query.encode()),
                    "concurrent_requests": args.concurrent_requests,
                    "reference_normalization": args.reference_normalization, "edges": 0, "coverage_rows": 0,
                    "provider": "offline deterministic synthetic noise; no semantic evaluation or network"},
                "seed_seconds": round(seed_seconds, 3),
                "snapshot": {"truncated": snapshot["truncated"], "bounded_active_and_historical": len(snapshot["nodes"]),
                    "passive_active_nodes": len(passive_snapshot["nodes"]),
                    "json_utf8_bytes": len(json.dumps(snapshot, ensure_ascii=False).encode())},
                "cold_foreground": {**quantiles(cold_ms), "provider_calls": 0, "hits": 0},
                "background_index": {**indexed, "seconds": round(index_seconds, 3), "provider_calls": index_calls},
                "hot_recall": {**quantiles(hot_ms), "provider_calls_during_samples": provider.calls - calls_before_hot,
                    "warmup_diagnostics": warm.diagnostics, "hit_counts": hit_counts, "diagnostics": diagnostics,
                    "profile_stages": {label: quantiles([sample.get(label, 0) for sample in stage_samples])
                                       for label in sorted({key for sample in stage_samples for key in sample})}},
                "concurrent_recall": {**quantiles(concurrent_ms), "hit_counts": concurrent_hits,
                    "deadline_count": sum(bool(item.get("deadline_exceeded")) for item in concurrent_diagnostics),
                    "diagnostics": concurrent_diagnostics, "event_loop_lag": quantiles(loop_lag_ms),
                    "recovered_hits": len(recovery.hits), "provider_calls_during_samples": provider.calls - calls_before_hot},
                "console_large_filtered_page": quantiles(large_read_ms),
                "console_small_spaces_filtered_pages": quantiles(small_read_ms),
                "cache_rows": cache_count["n"], "cache_vector_json_bytes": cache_count["bytes"],
                "files_before_close_bytes": open_files}
        finally:
            await cache.close()
            await memory.close()
        result["files_after_close_bytes"] = disk_bytes(directory)
        result["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=2100)
    parser.add_argument("--spaces", type=int, default=24)
    parser.add_argument("--dimension", type=int, default=768)
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument("--concurrent-requests", type=int, default=2)
    parser.add_argument("--text-repeat", type=int, default=1)
    parser.add_argument("--reference-normalization", action="store_true",
                        help="use the stage-six Python normalization for a same-workload comparison")
    args = parser.parse_args()
    if not 2001 <= args.nodes <= 10_000 or not 1 <= args.spaces <= 100 or not 16 <= args.dimension <= 4096 or not 3 <= args.repetitions <= 30 or not 1 <= args.concurrent_requests <= 8 or not 1 <= args.text_repeat <= 100:
        parser.error("bounded ranges: nodes 2001..10000, spaces 1..100, dimension 16..4096, repetitions 3..30, concurrent requests 1..8, text repeat 1..100")
    print(json.dumps(asyncio.run(run(args)), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
