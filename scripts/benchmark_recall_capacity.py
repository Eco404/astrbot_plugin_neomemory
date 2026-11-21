#!/usr/bin/env python3
"""Offline capacity evidence for bounded recall and final shared assembly.

The frozen workload uses two independent synthetic spaces and a hash-noise
embedding provider. It cannot measure semantic recall, network latency or live
AstrBot routing. Each CPU-affinity setting runs in a fresh subprocess. No
production setting is changed and no source conversation or API key is read.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import resource
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NODES = 2100
DIMENSION = 768
REPETITIONS = 10


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def source_identity():
    files = [ROOT / "main.py", Path(__file__), ROOT / "scripts/benchmark_console.py"]
    for directory in ("core", "storage", "adapters"):
        files.extend((ROOT / directory).rglob("*.py"))
    hashes = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in sorted(files)}
    return {"git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "source_tree_sha256": digest(hashes), "files": hashes}


def fixture(space_number):
    return [{"node_id": f"synthetic-{space_number}-{number:05d}",
             "status": "historical" if number % 4 == 0 else "active",
             # Unique importance fixes order independently of timestamps and UUIDs.
             "importance": .5 + number / 10000,
             "strength": .6,
             "content_text": f"合成空间 {space_number}，经历编号 {number:05d}。"
                 "我记录了一次独立的模拟经历。这段文字只用于测量存储、向量处理和完整条目装配开销，"
                 "不来自真实会话，也不代表向量模型具备语义检索能力。"}
            for number in range(NODES)]


def make_plan():
    return {"created_at_utc": datetime.now(UTC).isoformat(), "source": source_identity(),
            "fixture_sha256": digest([fixture(0), fixture(1)]),
            "workload": {"spaces": 2, "nodes_per_space": NODES,
                "active_per_space": 1575, "historical_per_space": 525,
                "dimension": DIMENSION, "current_queries": 1, "history_queries": 10,
                "history_seconds": 7200, "user_weight": .45, "agent_weight": .4,
                "deadline_seconds": 5.0, "budget_utf8_bytes": 2048,
                "index_limit": 2000, "batch_size": 16, "embedding_concurrency": 2,
                "repetitions": REPETITIONS, "edges": 0, "coverage_rows": 0,
                "profile": "one short profile per space", "relationship": "one short relationship per space",
                "scenarios": {"single": [0], "same_space_pair": [0, 0], "cross_space_pair": [0, 1]},
                "query_selection": "last active node plus previous 10 active nodes, exact text",
                "ordering": "unique importance descending; deterministic node IDs",
                "vector_provider": "offline hash-seeded noise; no network or semantic evaluation"},
            "affinity": {"all": sorted(os.sched_getaffinity(0)), "single": [min(os.sched_getaffinity(0))]},
            "method": {"quantiles": "nearest-rank; n=10 p95 equals maximum",
                "rss": "20ms thread samples of /proc/self/status plus Linux ru_maxrss process lifetime peak",
                "disk": "20ms sampled DB/WAL sizes plus explicit phase boundaries; not exact transient peaks",
                "event_loop": "10ms asyncio timer overshoot during each measured scenario",
                "deadline": "runtime-style concurrent local/recall, reserve25ms, final revalidation then synchronous assembly",
                "excluded": ["identity routing", "source history lookup", "name lookup", "reinforcement",
                    "short-term generation", "network", "semantic quality", "HTTP and browser", "dense graphs", "sustained load"],
                "cpu": "affinity is not exclusive CPU access or a low-spec hardware simulation"}}


def current_rss_bytes():
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("Linux VmRSS unavailable")


def files_bytes(directory):
    result = {}
    for path in Path(directory).iterdir():
        if path.is_file():
            try:
                result[path.name] = path.stat().st_size
            except FileNotFoundError:
                pass  # SQLite can remove its WAL between directory listing and stat.
    return result


class ResourceSamples:
    """Observe RSS outside the event loop so event-loop stalls remain visible."""

    def __init__(self, directory):
        self.directory = directory
        self.done = threading.Event()
        self.rss = []
        self.disk_max = {}
        self.disk_total_max = 0
        self.thread = threading.Thread(target=self.run, daemon=True)

    def sample(self):
        self.rss.append(current_rss_bytes())
        files = files_bytes(self.directory)
        self.disk_total_max = max(self.disk_total_max, sum(files.values()))
        for name, size in files.items():
            self.disk_max[name] = max(size, self.disk_max.get(name, 0))

    def run(self):
        while not self.done.wait(.02):
            self.sample()

    def start(self):
        self.sample()
        self.thread.start()

    def finish(self):
        self.done.set()
        self.thread.join()
        self.sample()
        return {"rss_before_bytes": self.rss[0], "rss_after_bytes": self.rss[-1],
                "rss_sampled_peak_bytes": max(self.rss), "rss_samples": len(self.rss),
                "process_lifetime_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                "files_sampled_max_bytes": self.disk_max,
                "files_simultaneous_sampled_peak_bytes": self.disk_total_max,
                "files_at_end_bytes": files_bytes(self.directory)}


async def monitor_loop(done, samples):
    while not done.is_set():
        before = time.perf_counter()
        await asyncio.sleep(.01)
        samples.append(max(0, (time.perf_counter() - before - .01) * 1000))


async def measured_request(contexts, recall, config, case):
    """Follow MemoryRuntime._passive scheduling without host/source adapters."""
    from core.context_bundle import assemble_context
    from core.retrieval import RecallResult

    started = time.perf_counter()
    deadline = time.monotonic() + config.recall_deadline_seconds
    tasks = [asyncio.create_task(contexts.snapshot(case["space_id"],
        logical_user_id=case["logical_user_id"], stream_id="synthetic", host_conversation_id=None)),
        asyncio.create_task(recall.recall(case["space_id"], case["query"],
            history=case["history"], retain_candidates=True))]
    report = {"space_number": case["space_number"], "has_content": False,
              "long_term_nodes": 0, "injected_bytes": 0, "deadline_exceeded": False}
    try:
        _, pending = await asyncio.wait(tasks, timeout=max(0, deadline - time.monotonic() - .025))
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        snapshot = None
        result = RecallResult(case["space_id"])
        for name, task in zip(("local", "recall"), tasks, strict=True):
            if task.cancelled():
                report[name + "_cancelled"] = True
                report["deadline_exceeded"] = True
            elif task.exception() is not None:
                report[name + "_error"] = type(task.exception()).__name__
            elif name == "local":
                snapshot = task.result()
            else:
                result = task.result()
        report["recall_diagnostics"] = result.diagnostics
        report["deadline_exceeded"] |= bool(result.diagnostics.get("deadline_exceeded"))
        report["ranked_candidates"] = len(result.candidates)
        if snapshot is None:
            snapshot = {"space_id": case["space_id"], "generation_id": result.generation_id,
                "profile": None, "relationship": None, "short_term": None, "active_notes": 0}
        before = time.perf_counter()
        async with asyncio.timeout(max(.025, deadline - time.monotonic())):
            snapshot, hits = await contexts.revalidate(snapshot,
                recalled_generation_id=result.generation_id, recalled_hits=result.candidates or result.hits)
        report["final_revalidate_ms"] = (time.perf_counter() - before) * 1000
        before = time.perf_counter()
        bundle = assemble_context(snapshot, hits, config, coverage=snapshot["recall_coverage"])
        report["final_assembly_ms"] = (time.perf_counter() - before) * 1000
        report.update(has_content=bool(bundle.text), long_term_nodes=len(bundle.hits),
            injected_bytes=len(bundle.text.encode()), context=bundle.diagnostics,
            selected_node_ids=[node["node_id"] for node in bundle.hits])
        assert report["injected_bytes"] <= config.recall_budget_tokens
        assert all(node["space_id"] == case["space_id"] for node in bundle.hits)
    except TimeoutError:
        report.update(final_revalidate_error="TimeoutError", deadline_exceeded=True)
    except AssertionError:
        raise
    except Exception as exc:  # noqa: BLE001 - retain runtime degradation evidence.
        report["assembly_error"] = type(exc).__name__
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    report["elapsed_ms"] = (time.perf_counter() - started) * 1000
    return report


async def worker(plan, affinity):
    # Imported only after subprocess affinity is set, including numerical workers.
    sys.path.insert(0, str(ROOT))
    from benchmark_console import (
        LocalContext,
        SyntheticEmbeddingProvider,
        hardware,
        quantiles,
    )

    from adapters.providers import ProviderAdapter
    from core.config import NeoMemoryConfig
    from core.embeddings import EmbeddingService
    from core.memory_spaces import private_space
    from core.retrieval import RecallPolicy, RecallService
    from storage.cache_store import CacheStore
    from storage.context_store import ContextStore
    from storage.memory_store import MemoryStore

    def timings(samples):
        return quantiles(samples) if samples else {"n": 0, "samples_ms": []}

    config = NeoMemoryConfig()
    policy = RecallPolicy()
    assert config.recall_deadline_seconds == policy.deadline_seconds
    assert config.recall_budget_tokens == policy.budget_tokens == 2048
    assert config.recall_index_limit == policy.index_limit == 2000
    assert config.history_limit == policy.history_limit == 10
    assert plan["source"] == source_identity()
    result = {"plan_sha256": digest(plan), "source_tree_sha256": plan["source"]["source_tree_sha256"],
              "affinity": affinity, "hardware": hardware(),
              "recall_policy": asdict(policy),
              "blas_environment": {key: os.environ.get(key) for key in
                  ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS")}}
    with tempfile.TemporaryDirectory(prefix="neomemory-recall-capacity-") as directory:
        memory = MemoryStore(Path(directory) / "memory.db", source_exists=lambda source: source == "synthetic-source")
        cache = CacheStore(Path(directory) / "cache.db")
        await memory.open()
        await cache.open()
        sampler = ResourceSamples(directory)
        sampler.start()
        try:
            provider = SyntheticEmbeddingProvider(DIMENSION)
            adapter = ProviderAdapter(LocalContext(provider), embedding_concurrency=2)
            embeddings = EmbeddingService(adapter, cache, provider_id="offline-synthetic", batch_size=16)
            recall = RecallService(memory, embeddings, policy=policy)
            contexts = ContextStore(memory)
            cases, candidates = [], []
            for space_number in range(2):
                user = f"synthetic-user-{space_number}"
                space = await memory.get_or_create_space(private_space("synthetic-persona", user))
                records = fixture(space_number)
                for record in records:
                    await memory.create_node(space_id=space["space_id"], node_type="event",
                        source_ids=["synthetic-source"], **record)
                await memory.upsert_profile(space_id=space["space_id"], logical_user_id=user,
                    tags=["合成材料"], description="这份短画像是独立容量实验的固定输入。")
                await memory.upsert_relationship(space_id=space["space_id"], logical_user_id=user,
                    state_text="我把这次模拟交流当作一段平静的相处。")
                shared = await memory.retrieval_snapshot(space["space_id"], include_historical=True)
                passive = await memory.retrieval_snapshot(space["space_id"])
                assert {node["node_id"] for node in shared["nodes"]} == {record["node_id"] for record in records[100:]}
                assert len(passive["nodes"]) == 1500
                assert shared["truncated"] and passive["truncated"]
                candidates.append({"space_number": space_number, "same_active_generation_total": NODES,
                    "shared_cap_selected": len(shared["nodes"]), "cap_omitted": 100,
                    "cap_omitted_fraction": 100 / NODES, "active_total": 1575,
                    "passive_selected": len(passive["nodes"]), "active_cap_omitted": 75,
                    "active_cap_omitted_fraction": 75 / 1575, "historical_removed_after_cap": 500,
                    "explanation": "count excluded by candidate cap, not semantic false-negative rate"})
                indexed = await recall.index_space(space["space_id"], timeout_seconds=120)
                assert indexed == {"indexed": 2000, "cached": 0, "truncated": True}
                active = [item for item in records if item["status"] == "active"]
                now = time.time()
                cases.append({"space_number": space_number, "space_id": space["space_id"], "logical_user_id": user,
                    "query": active[-1]["content_text"], "history": [
                        {"content_text": item["content_text"], "actor_kind": "user" if index % 2 == 0 else "agent",
                         "observed_at": now - index - 1}
                        for index, item in enumerate(active[-11:-1])]})
            result["candidate_boundary"] = candidates
            result["warmup"] = [await measured_request(contexts, recall, config, case) for case in cases]
            result["setup_resources"] = sampler.finish()
            calls_before = provider.calls
            result["scenarios"] = {}
            for name, order in plan["workload"]["scenarios"].items():
                sampler = ResourceSamples(directory)
                sampler.start()
                samples, groups, loop_lag = [], [], []
                monitor_done = asyncio.Event()
                monitor_task = asyncio.create_task(monitor_loop(monitor_done, loop_lag))
                try:
                    for _ in range(REPETITIONS):
                        before = time.perf_counter()
                        samples.extend(await asyncio.gather(*(measured_request(contexts, recall, config, cases[index])
                            for index in order)))
                        groups.append((time.perf_counter() - before) * 1000)
                finally:
                    monitor_done.set()
                    await monitor_task
                resources = sampler.finish()
                assert provider.calls == calls_before
                omitted = Counter(item["reason"] for sample in samples
                    for item in sample.get("context", {}).get("long_term_omitted", []))
                result["scenarios"][name] = {"requests": timings([sample["elapsed_ms"] for sample in samples]),
                    "groups": timings(groups), "event_loop_lag": timings(loop_lag),
                    "final_revalidate": timings([sample["final_revalidate_ms"] for sample in samples if "final_revalidate_ms" in sample]),
                    "final_assembly": timings([sample["final_assembly_ms"] for sample in samples if "final_assembly_ms" in sample]),
                    "has_content_fraction": sum(sample["has_content"] for sample in samples) / len(samples),
                    "has_long_term_fraction": sum(sample["long_term_nodes"] > 0 for sample in samples) / len(samples),
                    "deadline_count": sum(sample["deadline_exceeded"] for sample in samples),
                    "error_count": sum(any(key.endswith("_error") for key in sample) for sample in samples),
                    "provider_calls": provider.calls - calls_before,
                    "shared_budget_omission_counts": dict(omitted), "resources": resources, "samples": samples}
            result["recovery"] = [await measured_request(contexts, recall, config, case) for case in cases]
            rows = await cache.database.fetchone("SELECT COUNT(*) count,SUM(LENGTH(vector_json)) bytes FROM embedding_cache")
            result["cache"] = dict(rows)
            result["files_before_close_bytes"] = files_bytes(directory)
        finally:
            sampler.done.set()
            sampler.thread.join()
            await cache.close()
            await memory.close()
        result["files_after_close_bytes"] = files_bytes(directory)
        result["process_lifetime_peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        result["source_still_matches_plan"] = plan["source"] == source_identity()
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--worker", choices=("all", "single"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        plan = json.loads((args.output / "plan.json").read_text())
        os.sched_setaffinity(0, plan["affinity"][args.worker])
        save(args.output / f"{args.worker}.json", asyncio.run(worker(plan, args.worker)))
        return
    args.output.mkdir(parents=True, exist_ok=False)
    plan = make_plan()
    save(args.output / "plan.json", plan)
    print(f"Frozen plan SHA256: {digest(plan)}", flush=True)
    for affinity in ("all", "single"):
        subprocess.run([sys.executable, str(Path(__file__).resolve()), "--output", str(args.output.resolve()),
                        "--worker", affinity], check=True)
        print(f"Completed affinity={affinity}", flush=True)
    print(f"Results: {args.output.resolve()}", flush=True)


if __name__ == "__main__":
    main()
