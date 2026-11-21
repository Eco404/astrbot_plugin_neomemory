"""Opt-in independent schema correction through real AstrBot providers and stores.

Seeds are deliberately invalid fixtures, not natural model failure estimates.
Each case gets exactly one correction request, no transport or content retries.
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

PERSONA = "我是澄禾，认真倾听并区分约定与已经发生的事实。"
SOURCE = "我计划下周检查温室传感器；只有周三前收到校准件才开始，没到就延期。我有点担心测量偏差。"
CONTENT = "{{user:u1}}计划下周检查温室传感器，只有周三前收到校准件才开始，否则延期；检查尚未开始。"
CASES = ("emotion_enum", "empty_time", "local_ref")


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def candidate(kind):
    node = {"local_ref": "local_sensor", "node_type": "event", "operation": "create", "node_id": None,
            "expected_version": None, "title": "有条件的传感器检查计划", "content": CONTENT,
            "base_node_ids": [], "importance": .5, "source_ids": ["s1"], "event_time": None,
            "emotion": {"tags": [{"tag": "concern", "intensity": .25}], "description": "{{user:u1}}担心测量偏差。"}}
    if kind == "emotion_enum":
        node["emotion"]["tags"][0]["tag"] = "worried"
    elif kind == "empty_time":
        node["event_time"] = {"start": None, "end": None, "text": None}
    elif kind == "local_ref":
        node["local_ref"] = "local_温室"
    return {"schema_version": "extraction.v1", "result": "changes", "reason": "changes", "node_changes": [node],
            "edges": [], "coverage_changes": [], "profile_changes": [], "relationship_change": None}


async def run(directory, secrets, models):
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
    from astrbot.core.star.context import Context
    from adapters.providers import ProviderAdapter
    from core.extraction_apply import ExtractionService
    from core.extraction_references import ExtractionReferences, VERSION as REFERENCE_VERSION
    from core.extraction_regeneration import validation_feedback
    from core.prompts import schema_for_prompt_version
    from core.identity import EventIdentity
    from core.memory_spaces import private_space
    from core.source_ingest import CapturedSource
    from storage.memory_store import MemoryStore
    from storage.source_store import SourceStore

    report = {"synthetic_only": True, "natural_failure_rate": None, "http_requests": 0, "checks": [],
              "semantic_review": "pending", "models": models}
    cfg = secrets["ecoapi"]
    for model_index, model in enumerate(models):
        provider = ProviderOpenAIOfficial({"id": "probe", "type": "openai_chat_completion", "model": model,
            "api_base": cfg["base_url"], "key": [cfg["api_token"]], "timeout": 120}, {})
        provider.client.max_retries = 0
        async def count(_request):
            if report["http_requests"] >= len(models) * len(CASES):
                raise RuntimeError("probe_http_limit")
            report["http_requests"] += 1
        provider.client._client.event_hooks["request"].append(count)
        async def lookup(identifier):
            return provider if identifier == "probe" else None
        host = object.__new__(Context)
        host.provider_manager = SimpleNamespace(inst_map={"probe": provider}, provider_insts=[provider],
            embedding_provider_insts=[], get_provider_by_id=lookup)
        original = host.llm_generate
        async def generate(**kwargs):
            return await original(**kwargs, request_max_retries=1)
        host.llm_generate = generate
        adapter = ProviderAdapter(host)
        try:
            for kind in CASES:
                step = directory / f"{model_index}-{kind}"
                step.mkdir()
                sources = SourceStore(step / "source.db")
                memory = MemoryStore(step / "memory.db", source_exists=sources.has_source)
                await sources.open(); await memory.open()
                result = {"model": model, "case": kind}
                start = time.monotonic()
                try:
                    identity = EventIdentity("schema-probe", "synthetic", "FriendMessage", "probe:FriendMessage:visitor",
                        "visitor", "", "visitor", "访客", "chenghe", "1")
                    source = await sources.capture(CapturedSource(identity, "user", SOURCE, 1794787200.,
                        actor_account_id="visitor", actor_account_name="访客", persona_id="synthetic-chenghe"))
                    space = await memory.get_or_create_space(private_space("synthetic-chenghe", source.logical_user_id))
                    service = ExtractionService(sources, memory)
                    batch = await service.prepare(space_id=space["space_id"], source_ids=(source.source_id,),
                        persona=PERSONA, reference_protocol=REFERENCE_VERSION)
                    refs = ExtractionReferences(batch)
                    raw = json.dumps(candidate(kind), ensure_ascii=False)
                    feedback = validation_feedback({"error_code": "invalid_output", "window": {"raw": raw}},
                        schema_version=schema_for_prompt_version(batch.prompt_version))
                    messages = [*refs.messages(), {"role": "assistant", "content": raw},
                                {"role": "user", "content": json.dumps(feedback, ensure_ascii=False)}]
                    save(step / "request.json", messages)
                    answer = await adapter.generate(messages, provider_id="probe", timeout_seconds=120)
                    (step / "candidate.txt").write_text(answer, encoding="utf-8")
                    canonical = refs.decode(answer)
                    receipt = await service.apply(batch, canonical, model_id=model)
                    save(step / "receipt.json", receipt)
                    save(step / "nodes.json", await memory.list_nodes(space_id=space["space_id"]))
                    result["committed"] = True
                except Exception as exc:
                    result.update(failure(exc))
                finally:
                    result["seconds"] = time.monotonic() - start
                    await memory.close(); await sources.close()
                    report["checks"].append(result)
                    save(directory / "report.json", report)
        finally:
            await provider.terminate()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    parser.add_argument("--models", nargs="+", default=["gpt-6-luna", "gpt-5.6-luna"])
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "host/data/config").mkdir(parents=True)
    os.environ["ASTRBOT_ROOT"] = str((args.output / "host").resolve())
    from core.prompts import PROMPT_VERSION
    save(args.output / "manifest.json", {"persona": PERSONA, "source": SOURCE, "models": args.models,
        "cases": {kind: candidate(kind) for kind in CASES}, "prompt": PROMPT_VERSION,
        "review": ["保留用户主体", "保留周三前收到校准件的必要条件和否则延期", "不得声称已经检查完成"],
        "http_limit": len(args.models) * len(CASES), "content_retries": 0})
    logging.disable(logging.CRITICAL)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from loguru import logger
        logger.remove()
        result = asyncio.run(run(args.output, credentials(args.credentials), args.models))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if all(item.get("committed") for item in result["checks"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
