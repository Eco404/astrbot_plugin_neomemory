"""Opt-in synthetic automatic candidate recovery through real durable jobs.

The first response is an invalid fixture; subsequent responses use a real host
Provider. No explicit regeneration, network retry, source edit or candidate
patch is used. Results measure injected-fault recovery, not natural error rate.
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

PERSONA = "我是澄禾，温和认真，会陪对方讨论具体安排，也会分清计划和实际进展。"
SOURCES = (
    ("user", "我想修那台旧收音机，但只有借到校准仪才开工，否则继续等。现在还没动手。"),
    ("user", "还是之前那台收音机，今天已经预约了维修台。仪器还没借到，所以我仍没开工。我有点担心调错频率。"),
    ("agent", "那就先等校准仪吧，我愿意陪你慢慢准备，不用急着开始。"),
)
OLD_CONTENT = "{{user:u1}}想维修旧收音机，只有借到校准仪才开工，否则继续等待，目前尚未动手。"
NEW_CONTENT = "{{user:u1}}为之前那台旧收音机预约了维修台，但仍未借到校准仪、尚未开工；只有借到仪器才开始，否则继续等待。"
CASES = ("empty_time", "create_fields", "user_as_node", "expected_version", "relationship_evidence", "local_edge",
         "relationship_length", "node_length", "empty_title", "emotion_count", "importance_bound",
         "importance_type", "duplicate_sources", "neutral_conflict", "result_mismatch")


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def candidate(kind):
    from core.extraction_schema import SCHEMA_VERSION
    from core.relationship_state import LONG_TERM_MAX_CHARS
    node = {"local_ref": "local_1", "node_type": "event", "operation": "update", "node_id": "n1",
            "expected_version": 1, "title": "旧收音机维修准备", "content": NEW_CONTENT,
            "importance": .5, "source_ids": ["s2"], "event_time": None, "base_node_ids": [],
            "emotion": {"tags": [{"tag": "concern", "intensity": .25}], "description": "{{user:u1}}担心调错频率。"}}
    result = {"schema_version": SCHEMA_VERSION, "result": "changes", "reason": "changes", "node_changes": [node],
              "edges": [], "coverage_changes": [], "profile_changes": [], "relationship_change": None}
    if kind == "empty_time":
        node["event_time"] = {"start": None, "end": None, "text": None}
    elif kind == "create_fields":
        node["operation"] = "create"
    elif kind == "user_as_node":
        node["node_id"] = "u1"
    elif kind == "expected_version":
        node["expected_version"] = 2
    elif kind == "relationship_evidence":
        result["relationship_change"] = {"logical_user_id": "u1", "expected_version": 0,
            "long_term": {"op": "replace", "body": "他愿意告诉我准备中的顾虑，我想陪他慢慢推进，别让这件事变成催促。"},
            "aftereffect": {"op": "keep", "body": ""}, "source_ids": ["s3"]}
    elif kind == "local_edge":
        result["edges"] = [{"from_ref": "local_1", "to_ref": "local_undeclared", "relation_type": "related", "source_ids": ["s2"]}]
    elif kind == "relationship_length":
        text = "他愿意说起顾虑，我想耐心陪他准备，不能把关心变成催促。"
        result["relationship_change"] = {"logical_user_id": "u1", "expected_version": 0,
            "long_term": {"op": "replace", "body": text * (LONG_TERM_MAX_CHARS // len(text) + 1)},
            "aftereffect": {"op": "keep", "body": ""}, "source_ids": ["s2", "s3"]}
    elif kind == "node_length":
        node["content"] = NEW_CONTENT * 80
    elif kind == "empty_title":
        node["title"] = ""
    elif kind == "emotion_count":
        node["emotion"]["tags"] = [{"tag": tag, "intensity": .25} for tag in ["concern", "fear", "sadness", "trust"]]
    elif kind == "importance_bound":
        node["importance"] = 1.1
    elif kind == "importance_type":
        node["importance"] = "0.5"
    elif kind == "duplicate_sources":
        node["source_ids"] = ["s2", "s2"]
    elif kind == "neutral_conflict":
        node["emotion"]["tags"] = [{"tag": tag, "intensity": .25} for tag in ["neutral", "concern"]]
    elif kind == "result_mismatch":
        result["result"] = "no_change"
    else:
        raise ValueError("unknown synthetic case")
    return result


class SeedThenProvider:
    def __init__(self, real, kind, directory):
        self.real, self.kind, self.directory = real, kind, directory
        self.calls = 0

    def llm_fingerprint(self, *args, **kwargs):
        return self.real.llm_fingerprint(*args, **kwargs)

    async def generate(self, messages, **kwargs):
        self.calls += 1
        save(self.directory / f"request-{self.calls}.json", messages)
        if self.calls == 1:
            raw = json.dumps(candidate(self.kind), ensure_ascii=False)
        else:
            raw = await self.real.generate(messages, **kwargs)
        (self.directory / f"response-{self.calls}.json").write_text(raw, encoding="utf-8")
        return raw


async def run(directory, secrets, model, cases=CASES):
    from astrbot.core.provider.sources.openai_responses_source import ProviderOpenAIResponses
    from astrbot.core.star.context import Context
    from adapters.providers import ProviderAdapter
    from core.config import NeoMemoryConfig
    from core.identity import EventIdentity
    from core.maintenance import MaintenanceEngine
    from core.memory_spaces import private_space
    from core.source_ingest import CapturedSource
    from storage.memory_store import MemoryStore
    from storage.source_store import SourceStore

    report = {"synthetic_only": True, "natural_error_rate": None, "model": model,
              "provider": "openai_responses", "output_format": "text", "http_requests": 0,
              "http_limit": 3 * len(cases), "manual_regenerations": 0, "checks": [], "semantic_review": "pending"}
    cfg = secrets["ecoapi"]
    provider = ProviderOpenAIResponses({"id": "synthetic-recovery", "type": "openai_responses", "model": model,
        "api_base": cfg["base_url"], "key": [cfg["api_token"]], "timeout": 120}, {})
    provider.client.max_retries = 0

    async def count(_request):
        if report["http_requests"] >= report["http_limit"]:
            raise RuntimeError("synthetic_recovery_http_limit")
        report["http_requests"] += 1

    provider.client._client.event_hooks["request"].append(count)
    async def lookup(identifier):
        return provider if identifier == "synthetic-recovery" else None
    host = object.__new__(Context)
    host.provider_manager = SimpleNamespace(inst_map={"synthetic-recovery": provider}, provider_insts=[provider],
        embedding_provider_insts=[], get_provider_by_id=lookup)
    original = host.llm_generate
    async def generate(**kwargs):
        return await original(**kwargs, request_max_retries=1)
    host.llm_generate = generate
    real = ProviderAdapter(host, llm_concurrency=1)
    try:
        for kind in cases:
            step = directory / kind
            step.mkdir()
            sources = SourceStore(step / "source.db")
            memory = MemoryStore(step / "memory.db", source_exists=sources.has_source)
            await sources.open(); await memory.open()
            engine = None
            start = time.monotonic()
            before_http = report["http_requests"]
            check = {"case": kind}
            try:
                records = []
                for index, (actor, content) in enumerate(SOURCES):
                    identity = EventIdentity("recovery-probe", "synthetic", "FriendMessage", "probe:FriendMessage:visitor",
                        "visitor", "", "visitor", "访客", "chenghe", str(index))
                    records.append(await sources.capture(CapturedSource(identity, actor, content, 1794873600. + index,
                        actor_account_id="visitor" if actor == "user" else "chenghe",
                        actor_account_name="访客" if actor == "user" else "澄禾", persona_id="synthetic-chenghe")))
                user = records[0].logical_user_id
                space = await memory.get_or_create_space(private_space("synthetic-chenghe", user))
                old = await memory.create_node(space_id=space["space_id"], node_type="event", title="旧收音机维修计划",
                    content_text=OLD_CONTENT.replace("{{user:u1}}", "{{user:" + user + "}}"),
                    source_ids=[records[0].source_id], importance=.5)
                adapter = SeedThenProvider(real, kind, step)
                runtime = SimpleNamespace(adapter=adapter, recall=None, cache=None)
                config = NeoMemoryConfig(llm_provider_id="synthetic-recovery", extraction_auto_regeneration=True,
                    extraction_max_regenerations=3, extraction_output_format="text", extraction_timeout_seconds=120,
                    private_min_messages=3, private_topic_messages=4, private_max_messages=5,
                    job_max_attempts=1, job_lease_seconds=30)
                async def persona_loader(_persona):
                    return PERSONA
                engine = MaintenanceEngine(sources, memory, runtime, config, persona_loader)
                queued = await engine.enqueue("extract", space["space_id"], "synthetic-" + kind)
                claimed = await engine.jobs.claim("synthetic-recovery-worker", lease_seconds=30)
                assert claimed["job_id"] == queued["job_id"]
                await engine.runner._execute(claimed)
                job = await engine.jobs.get(queued["job_id"])
                archives = await engine.jobs.list_candidates(job["job_id"])
                nodes = await memory.list_nodes(space_id=space["space_id"])
                relation = await memory.get_relationship(space_id=space["space_id"], logical_user_id=user)
                generation = await memory.get_active_generation(space["space_id"])
                consumed = await engine.pipeline.consumed(generation["generation_id"], [r.source_id for r in records])
                save(step / "job.json", job)
                save(step / "archives.json", archives)
                save(step / "nodes.json", nodes)
                save(step / "relationship.json", relation)
                save(step / "profiles.json", await memory.list_profiles(space_id=space["space_id"]))
                save(step / "source-rows.json", await sources.get_records(tuple(r.source_id for r in records)))
                check.update(state=job["state"], error_code=job["error_code"], calls_including_seed=adapter.calls,
                    archived_candidates=len(archives), consumed=len(consumed), activity_round=generation["activity_round"],
                    old_node_version=(await memory.get_node(old["node_id"]))["content_version"], node_count=len(nodes),
                    actual_http_requests=report["http_requests"] - before_http,
                    error_types=[error["type"] for archive in reversed(archives)
                                 for error in archive["window"].get("validation_errors", [])])
                check["committed_once"] = job["state"] == "succeeded" and len(consumed) == 3 and generation["activity_round"] == 1
            except Exception as exc:
                check.update(failure(exc))
            finally:
                check["seconds"] = round(time.monotonic() - start, 3)
                report["checks"].append(check)
                save(directory / "report.json", report)
                if engine is not None:
                    await engine.stop()
                await memory.close(); await sources.close()
    finally:
        await provider.terminate()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "host/data/config").mkdir(parents=True)
    os.environ["ASTRBOT_ROOT"] = str((args.output / "host").resolve())
    from core.prompts import PROMPT_VERSION
    from core.extraction_regeneration import VERSION
    save(args.output / "manifest.json", {"synthetic_only": True, "persona": PERSONA, "sources": SOURCES,
        "cases": {kind: candidate(kind) for kind in args.cases}, "model": args.model, "prompt": PROMPT_VERSION,
        "regeneration_protocol": VERSION, "first_response": "injected_invalid_fixture", "max_corrections": 3,
        "http_limit": 3 * len(args.cases), "manual_regeneration": False, "network_retries": False,
        "semantic_checks": ["同一台旧收音机的准备进展，不应另建重复事件", "预约已完成但维修未开始",
            "借到校准仪才开工，否则等待的条件保留", "用户安排与担心不可写为人格自身经历",
            "关系可以不更新；如更新必须引用相关用户来源，不使用无关来源凑数", "不得创建无依据关系边或补造时间"]})
    logging.disable(logging.CRITICAL)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from loguru import logger
        logger.remove()
        result = asyncio.run(run(args.output, credentials(args.credentials), args.model, args.cases))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if all(row.get("committed_once") for row in result["checks"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
