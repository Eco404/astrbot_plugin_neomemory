"""SIGKILL synthetic maintenance workers after deterministic memory commits.

Uses real stores, extraction application, job leases and rebuild switching with
one deterministic in-process Provider response. No host or network is involved.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import platform
import signal
import sqlite3
import subprocess
import sys
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from verify_ingress_sigkill import emit, synthetic_source

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.config import NeoMemoryConfig  # noqa: E402
from core.maintenance import MaintenanceEngine  # noqa: E402
from core.memory_spaces import private_space  # noqa: E402
from storage.memory_store import MemoryStore  # noqa: E402
from storage.source_store import SourceStore  # noqa: E402

CASES = ("extraction_committed", "rebuild_window_committed", "rebuild_activated")
CONTENT_TABLES = (
    "memory_nodes", "memory_node_versions", "node_sources", "user_profiles", "profile_versions",
    "relationship_states", "relationship_versions", "memory_derivations", "memory_derivation_sources",
    "memory_derivation_context_sources",
    "extraction_batches", "extraction_batch_sources", "derived_cursors",
)


class SyntheticProvider:
    def __init__(self, *, allow_generate):
        self.allow_generate = allow_generate
        self.calls = 0

    def llm_fingerprint(self, provider_id, revision=""):
        return "synthetic-sigkill:v1"

    async def generate(self, messages, **kwargs):
        self.calls += 1
        assert self.allow_generate, "A committed receipt must avoid another Provider call"
        payload = json.loads(next(message["content"] for message in messages if message["role"] == "user"))
        source_ids = [source["source_id"] for source in payload["sources"]]
        logical_user = payload["space"]["logical_user_id"]
        person = "{{user:" + logical_user + "}}"
        return json.dumps({
            "schema_version": "extraction.v1", "result": "changes", "reason": "changes",
            "node_changes": [{"local_ref": "local_music", "node_type": "event", "operation": "create",
                "node_id": None, "expected_version": None, "content": f"我记得{person}计划练习长笛，并希望我给予鼓励。",
                "title": "长笛练习", "emotion": None, "base_node_ids": [],
                "importance": .6, "source_ids": source_ids, "event_time": None}],
            "profile_changes": [{"logical_user_id": logical_user, "expected_version": 0,
                "tags": ["长笛"], "description": f"{person}正在练习长笛。", "source_ids": source_ids}],
            "relationship_change": {"logical_user_id": logical_user, "expected_version": 0,
                "long_term": {"op": "replace", "body": "我想耐心鼓励对方练习音乐。"},
                "aftereffect": {"op": "keep", "body": ""}, "source_ids": source_ids}, "edges": [], "coverage_changes": [],
        }, ensure_ascii=False)


async def environment(directory, *, allow_generate):
    sources = SourceStore(directory / "source.db")
    await sources.open()
    memory = MemoryStore(directory / "memory.db", source_exists=sources.has_source)
    await memory.open()
    adapter = SyntheticProvider(allow_generate=allow_generate)
    config = NeoMemoryConfig(llm_provider_id="synthetic", private_min_messages=2,
        private_topic_messages=3, private_max_messages=4, private_idle_seconds=30,
        topic_consolidation_enabled=False)

    async def persona_loader(persona_id):
        assert persona_id == "synthetic-persona"
        return "我是一位耐心鼓励音乐练习的合成陪伴者。"

    engine = MaintenanceEngine(sources, memory, SimpleNamespace(adapter=adapter, recall=None, cache=None),
        config, persona_loader)
    return SimpleNamespace(sources=sources, memory=memory, engine=engine, adapter=adapter)


async def content_snapshot(memory, generation_id=None):
    nodes = "SELECT node_id FROM memory_nodes WHERE generation_id=?"
    versions = f"SELECT node_version_id FROM memory_node_versions WHERE node_id IN ({nodes})"
    profiles = "SELECT profile_id FROM user_profiles WHERE generation_id=?"
    profile_versions = f"SELECT profile_version_id FROM profile_versions WHERE profile_id IN ({profiles})"
    relationships = "SELECT relationship_id FROM relationship_states WHERE generation_id=?"
    relationship_versions = f"SELECT relationship_version_id FROM relationship_versions WHERE relationship_id IN ({relationships})"
    derivation_filter = (f"node_version_id IN ({versions}) OR profile_version_id IN ({profile_versions}) "
                         f"OR relationship_version_id IN ({relationship_versions})")
    derivations = f"SELECT derivation_id FROM memory_derivations WHERE {derivation_filter}"
    filters = {
        "memory_node_versions": f"node_id IN ({nodes})", "node_sources": f"node_id IN ({nodes})",
        "profile_versions": f"profile_id IN ({profiles})",
        "relationship_versions": f"relationship_id IN ({relationships})",
        "memory_derivations": derivation_filter,
        "memory_derivation_sources": f"derivation_id IN ({derivations})",
        "memory_derivation_context_sources": f"derivation_id IN ({derivations})",
    }
    result = {}
    for table in CONTENT_TABLES:
        condition = filters.get(table, "generation_id=?")
        query = f"SELECT * FROM {table}" + (f" WHERE {condition}" if generation_id is not None else "")
        params = (generation_id,) * condition.count("?") if generation_id is not None else ()
        rows = [dict(row) for row in await memory.database.fetchall(query, params)]
        encoded = sorted(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows)
        result[table] = {"count": len(rows), "sha256": hashlib.sha256("\n".join(encoded).encode()).hexdigest()}
    return result


async def state(env, job_id, space_id):
    job = await env.engine.jobs.get(job_id)
    generations = [dict(row) for row in await env.memory.database.fetchall(
        "SELECT generation_id,status,activity_round FROM memory_generations ORDER BY created_at,generation_id")]
    visible = await env.memory.list_nodes(space_id=space_id)
    return {"job_state": job["state"], "lease_until": job["lease_until"], "attempt": job["attempt"],
        "step": job["step"], "progress_done": job["progress_done"], "checkpoint_phase": job["checkpoint"].get("phase"),
        "generations": generations, "visible_node_ids": [node["node_id"] for node in visible],
        "content": await content_snapshot(env.memory),
        "generation_content": {row["generation_id"]: await content_snapshot(env.memory, row["generation_id"])
            for row in generations},
        "rebuild_receipt": await env.engine.pipeline.get_rebuild_receipt(job_id)}


async def writer(directory, case, lease_seconds):
    env = await environment(directory, allow_generate=True)
    texts = ("我计划练习长笛。", "希望你鼓励我的音乐练习。")
    records = [await env.sources.capture(replace(synthetic_source(number), content_text=text))
        for number, text in enumerate(texts, 1)]
    space = await env.memory.get_or_create_space(private_space("synthetic-persona", records[0].logical_user_id))
    parent = await env.memory.get_active_generation(space["space_id"])
    old_node = None
    if case != "extraction_committed":
        old_node = await env.memory.create_node(space_id=space["space_id"], node_type="event",
            content_text="重构前的合成记忆。", source_ids=[records[0].source_id])
    queued = await env.engine.enqueue("extract" if case == "extraction_committed" else "rebuild",
        space["space_id"], "synthetic-sigkill-job")
    job = await env.engine.jobs.claim("synthetic-writer", lease_seconds=lease_seconds)
    assert job and job["job_id"] == queued["job_id"]

    async def boundary():
        current = await state(env, job["job_id"], space["space_id"])
        assert current["job_state"] == "running"
        assert current["content"]["extraction_batches"]["count"] == 1
        assert env.adapter.calls == 1
        emit({"boundary": case, "pid": os.getpid(), "job_id": job["job_id"], "space_id": space["space_id"],
            "parent_generation_id": parent["generation_id"], "old_node_id": old_node["node_id"] if old_node else None,
            "state": current, "provider_calls": env.adapter.calls,
            "journal_mode": (await env.memory.database.fetchone("PRAGMA journal_mode"))[0],
            "synchronous": (await env.memory.database.fetchone("PRAGMA synchronous"))[0]})
        await asyncio.Event().wait()

    if case == "rebuild_activated":
        original = env.engine.pipeline.activate_rebuild

        async def activate_then_wait(*args, **kwargs):
            await original(*args, **kwargs)
            await boundary()

        env.engine.pipeline.activate_rebuild = activate_then_wait
    else:
        original = env.engine.extraction.apply

        async def apply_then_wait(*args, **kwargs):
            await original(*args, **kwargs)
            await boundary()

        env.engine.extraction.apply = apply_then_wait
    await env.engine.runner._execute(job, lease_seconds=lease_seconds)
    failed = await env.engine.jobs.get(job["job_id"])
    raise AssertionError(f"Writer did not reach boundary: {failed['state']} {failed['error_code']}")


async def recovery(directory, case, lease_seconds):
    barrier = json.loads((directory / "barrier.json").read_text())
    env = await environment(directory, allow_generate=False)
    try:
        before = await state(env, barrier["job_id"], barrier["space_id"])
        assert before["job_state"] == "running" and before["lease_until"] > time.time()
        assert before["content"] == barrier["state"]["content"]
        assert await env.engine.jobs.claim("too-early", lease_seconds=lease_seconds) is None
        parent_id = barrier["parent_generation_id"]
        active = next(row for row in before["generations"] if row["status"] == "active")
        if case == "rebuild_window_committed":
            assert active["generation_id"] == parent_id
            assert before["visible_node_ids"] == [barrier["old_node_id"]]
            assert before["rebuild_receipt"] is None
            assert any(row["status"] == "building" and row["activity_round"] == 1 for row in before["generations"])
        elif case == "rebuild_activated":
            assert active["generation_id"] != parent_id
            assert before["rebuild_receipt"]["target_generation_id"] == active["generation_id"]
            assert barrier["old_node_id"] not in before["visible_node_ids"]
        await asyncio.sleep(max(0, before["lease_until"] - time.time()) + .05)
        resumed = await env.engine.jobs.claim("synthetic-recovery", lease_seconds=lease_seconds)
        assert resumed and resumed["job_id"] == barrier["job_id"]
        await env.engine.runner._execute(resumed, lease_seconds=lease_seconds)
        completed = await env.engine.jobs.get(resumed["job_id"])
        assert completed["state"] == "succeeded", completed["error_code"]
        assert completed["attempt"] == 1 and completed["checkpoint"] == {}
        after = await state(env, resumed["job_id"], barrier["space_id"])
        if case == "rebuild_window_committed":
            target_id = next(row["generation_id"] for row in before["generations"] if row["status"] == "building")
            assert after["generation_content"][target_id] == before["generation_content"][target_id], (
                "Receipt replay changed committed target content or citations")
            assert all(item["count"] == 0 for item in after["generation_content"][parent_id].values())
            assert after["content"] == after["generation_content"][target_id], "Retired content remains stored"
        else:
            assert after["content"] == before["content"], "Receipt replay changed committed content or citations"
        active = next(row for row in after["generations"] if row["status"] == "active")
        assert active["activity_round"] == 1
        assert len(after["visible_node_ids"]) == 1
        if case != "extraction_committed":
            assert active["generation_id"] != parent_id
            assert barrier["old_node_id"] not in after["visible_node_ids"]
            assert await env.memory.get_node(barrier["old_node_id"]) is None
            assert all(item["count"] == 0 for item in after["generation_content"][parent_id].values())
            assert after["rebuild_receipt"]["target_generation_id"] == active["generation_id"]
            assert completed["progress_done"] == completed["progress_total"] == 2
        assert await env.engine.jobs.claim("finished", lease_seconds=lease_seconds) is None
        assert env.adapter.calls == 0
        checks = {}
        for name, store in (("memory", env.memory), ("source", env.sources)):
            checks[name] = {"integrity": [row[0] for row in await store.database.fetchall("PRAGMA integrity_check")],
                "foreign_key_violations": len(await store.database.fetchall("PRAGMA foreign_key_check"))}
            assert checks[name] == {"integrity": ["ok"], "foreign_key_violations": 0}
        return {"ok": True, "pid": os.getpid(), "unexpired_lease_blocks_takeover": True,
            "before": before, "after": after, "unchanged_content_tables": list(CONTENT_TABLES),
            "unchanged_content_scope": "target_generation" if case == "rebuild_window_committed" else "database",
            "retired_content_removed": case != "extraction_committed",
            "provider_calls": env.adapter.calls, "checks": checks}
    finally:
        await env.engine.stop()
        await env.memory.close()
        await env.sources.close()


async def launch_worker(directory, case, lease_seconds, worker):
    return await asyncio.create_subprocess_exec(sys.executable, str(Path(__file__).resolve()),
        "--worker", worker, "--case", case, "--output", str(directory), "--lease-seconds", str(lease_seconds),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)


async def terminate_owned(process):
    if process.returncode is None:
        process.kill()
        await process.wait()


async def run(directory, repeats, lease_seconds):
    directory.mkdir(parents=True, exist_ok=False)
    report = {"tested_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "source_helper_sha256": hashlib.sha256((ROOT / "scripts/verify_ingress_sigkill.py").read_bytes()).hexdigest(),
        "python": platform.python_version(), "sqlite": sqlite3.sqlite_version, "platform": platform.platform(),
        "scope": "synthetic_maintenance_sigkill_not_power_loss", "http_requests": 0,
        "test_lease_seconds": lease_seconds, "repeats": repeats, "cases": [], "ok": False}
    try:
        for case in CASES:
            for iteration in range(1, repeats + 1):
                case_dir = directory / f"{case}-{iteration}"
                case_dir.mkdir()
                writer_process = await launch_worker(case_dir, case, lease_seconds, "write")
                try:
                    line = await asyncio.wait_for(writer_process.stdout.readline(), 15)
                    if not line:
                        await writer_process.wait()
                        raise RuntimeError((await writer_process.stderr.read()).decode())
                    barrier = json.loads(line)
                    assert barrier["boundary"] == case
                    wal_bytes = (case_dir / "memory.db-wal").stat().st_size
                    assert wal_bytes > 0
                    writer_process.send_signal(signal.SIGKILL)
                    await asyncio.wait_for(writer_process.wait(), 5)
                    assert writer_process.returncode == -signal.SIGKILL
                    (case_dir / "barrier.json").write_text(json.dumps(barrier, indent=2) + "\n")
                    recovery_process = await launch_worker(case_dir, case, lease_seconds, "recover")
                    try:
                        stdout, stderr = await asyncio.wait_for(recovery_process.communicate(), lease_seconds + 15)
                        if recovery_process.returncode:
                            raise RuntimeError(stderr.decode())
                        result = json.loads(stdout)
                        assert result["pid"] != barrier["pid"]
                    finally:
                        await terminate_owned(recovery_process)
                    report["cases"].append({"case": case, "iteration": iteration, "barrier": barrier,
                        "writer_exit_code": writer_process.returncode, "wal_bytes_at_kill": wal_bytes, "recovery": result})
                    emit({"case": case, "iteration": iteration, "ok": result["ok"]})
                finally:
                    await terminate_owned(writer_process)
        report["ok"] = True
    finally:
        (directory / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New isolated output directory")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--lease-seconds", type=float, default=3)
    parser.add_argument("--worker", choices=("write", "recover"), help=argparse.SUPPRESS)
    parser.add_argument("--case", choices=CASES, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.repeats < 1 or not 0 < args.lease_seconds <= 30:
        parser.error("repeats must be positive; test lease must be in (0,30] seconds")
    if args.worker == "write":
        asyncio.run(writer(args.output.resolve(), args.case, args.lease_seconds))
    elif args.worker == "recover":
        emit(asyncio.run(recovery(args.output.resolve(), args.case, args.lease_seconds)))
    else:
        result = asyncio.run(run(args.output.resolve(), args.repeats, args.lease_seconds))
        emit({"ok": result["ok"], "case_count": len(result["cases"]), "report": str(args.output / "report.json")})


if __name__ == "__main__":
    main()
