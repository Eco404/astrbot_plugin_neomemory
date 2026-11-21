#!/usr/bin/env python3
"""Bounded synthetic checks of AstrBot Responses output-format transport.

Uses an isolated host provider, never a live plugin or shared model config.
Each mode gets one HTTP request; no content repair or retries are performed.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import copy
import io
import json
import logging
import os
import time
from dataclasses import replace
from pathlib import Path

from scripts.verify_stage4_api import credentials, failure

SOURCE = "我打算下周整理植物标本；只有周三前借到干燥箱才开始，没借到就延期。目前尚未开始。我有点担心标本受潮。"
PERSONA = "我是澄禾，认真倾听，会区分用户的计划、条件和已经发生的事。"


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


async def run(directory, secrets, model, *, scenario="basic", modes=("text", "json_object", "json_schema")):
    from astrbot.core.provider.sources.openai_responses_source import (
        ProviderOpenAIResponses,
    )

    from adapters.providers import _response_diagnostics, _structured_provider
    from core.extraction_apply import ExtractionService
    from core.extraction_references import VERSION, ExtractionReferences
    from core.extraction_regeneration import validation_feedback
    from core.prompts import schema_for_prompt_version
    from core.extraction_schema import extraction_json_schema
    from core.identity import EventIdentity
    from core.memory_spaces import private_space
    from core.source_ingest import CapturedSource
    from storage.memory_store import MemoryStore
    from storage.source_store import SourceStore

    report = {"synthetic_only": True, "model": model, "checks": [], "http_requests": 0,
              "natural_failure_rate": None, "semantic_review": "pending"}
    cfg = secrets["ecoapi"]
    selected = ProviderOpenAIResponses({"id": "synthetic-responses", "type": "openai_responses",
        "model": model, "api_base": cfg["base_url"], "key": [cfg["api_token"]], "timeout": 120}, {})
    original = copy.deepcopy(selected.provider_config)
    sources = SourceStore(directory / "source.db")
    memory = MemoryStore(directory / "memory.db", source_exists=sources.has_source)
    await sources.open(); await memory.open()
    identity = EventIdentity("structure-probe", "synthetic", "FriendMessage", "probe:FriendMessage:visitor",
                             "visitor", "", "visitor", "访客", "chenghe", "1")
    source = await sources.capture(CapturedSource(identity, "user", SOURCE, 1794873600.,
        actor_account_id="visitor", actor_account_name="访客", persona_id="synthetic-chenghe"))
    space = await memory.get_or_create_space(private_space("synthetic-chenghe", source.logical_user_id))
    source_ids = [source.source_id]
    known_ids = []
    if scenario != "basic":
        for index in range(16):
            content = (f"我在第{index + 1}次社区档案志愿活动中整理了第{index + 1}号柜的旧地图。"
                       "当次整理已结束，登记过地图的纸张状态和存放位置，尚未开展数字化。"
                       "这些活动各自独立，不是同一次活动的进度更新。")
            historic = await sources.capture(CapturedSource(replace(identity, message_id=f"past-{index}"),
                "user", content, 1794873600. - (index + 1) * 86400,
                actor_account_id="visitor", actor_account_name="访客", persona_id="synthetic-chenghe"))
            node = await memory.create_node(space_id=space["space_id"], node_type="event",
                content_text=content.replace("我", "{{user:" + source.logical_user_id + "}}", 1),
                source_ids=(historic.source_id,), title=f"第{index + 1}次地图整理")
            known_ids.append(node["node_id"])
        for index, content in enumerate((
            "另一件事：昨天我完成了图书馆志愿登记，正式值班要等排班通知，目前还没有值班。",
            "还有，今天我已把维修好的自行车取回，刹车已经试过正常；这与植物标本和图书馆安排无关。")):
            extra = await sources.capture(CapturedSource(replace(identity, message_id=f"extra-{index}"),
                "user", content, 1794873601. + index, actor_account_id="visitor", actor_account_name="访客",
                persona_id="synthetic-chenghe"))
            source_ids.append(extra.source_id)
    service = ExtractionService(sources, memory)
    batch = await service.prepare(space_id=space["space_id"], source_ids=tuple(source_ids),
                                  known_node_ids=tuple(known_ids), persona=PERSONA, reference_protocol=VERSION)
    refs = ExtractionReferences(batch)
    messages = refs.messages()
    if scenario == "repair":
        from scripts.probe_regeneration_constraints import candidate
        draft = candidate("local_ref")
        draft["node_changes"][0].update(content=SOURCE.replace("我", "{{user:u1}}"), title="植物标本整理计划", emotion=None)
        raw = json.dumps(draft, ensure_ascii=False)
        messages.extend([{"role": "assistant", "content": raw}, {"role": "user", "content":
            json.dumps(validation_feedback({"error_code": "invalid_output", "window": {"raw": raw}},
                        schema_version=schema_for_prompt_version(batch.prompt_version)), ensure_ascii=False)}])
    save(directory / "request.json", messages)
    try:
        for mode in modes:
            provider = selected if mode == "text" else _structured_provider(selected, mode, extraction_json_schema())
            provider.client.max_retries = 0
            check = {"mode": mode, "http_requests": 0}
            async def capture(request, check=check):
                if check["http_requests"] >= 1:
                    raise RuntimeError("probe_http_limit")
                check["http_requests"] += 1
                report["http_requests"] += 1
                payload = json.loads(request.content)
                check["sent_format"] = payload.get("text", {}).get("format", {}).get("type", "text")
                check["schema_strict"] = payload.get("text", {}).get("format", {}).get("strict")
                check["input_roles"] = [item.get("role") for item in payload.get("input", [])]
            provider.client._client.event_hooks["request"].append(capture)
            started = time.monotonic()
            try:
                async with asyncio.timeout(125):
                    response = await provider.text_chat(contexts=messages, request_max_retries=1)
                check.update(_response_diagnostics(response))
                raw = response.completion_text
                (directory / (mode + ".txt")).write_text(raw, encoding="utf-8")
                canonical = refs.decode(raw)
                save(directory / (mode + ".json"), json.loads(canonical))
                check["schema_valid"] = True
            except Exception as exc:  # noqa: BLE001 - probe reports model and transport failures without secrets
                check.update(failure(exc)); check["schema_valid"] = False
            finally:
                provider.client._client.event_hooks["request"].remove(capture)
                if provider is not selected:
                    await provider.terminate()
                check["seconds"] = round(time.monotonic() - started, 3)
                report["checks"].append(check)
                report["shared_config_unchanged"] = selected.provider_config == original
                save(directory / "report.json", report)
    finally:
        await selected.terminate(); await memory.close(); await sources.close()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--scenario", choices=("basic", "long", "repair"), default="basic")
    parser.add_argument("--modes", nargs="+", choices=("text", "json_object", "json_schema"),
                        default=["text", "json_object", "json_schema"])
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "host/data/config").mkdir(parents=True)
    os.environ["ASTRBOT_ROOT"] = str((args.output / "host").resolve())
    save(args.output / "manifest.json", {"source": SOURCE, "persona": PERSONA,
        "model": args.model, "scenario": args.scenario, "modes": args.modes, "http_limit": len(args.modes),
        "review": ["用户计划，不是人格行动", "下周", "周三前借到干燥箱才开始", "否则延期", "尚未开始", "担忧标本受潮",
                   "扩展场景中三项独立经历不合并，旧地图节点不被改写", "登记已完成但未值班，自行车已取回并试过刹车"]})
    logging.disable(logging.CRITICAL)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from loguru import logger
        logger.remove()
        result = asyncio.run(run(args.output, credentials(args.credentials), args.model,
                                scenario=args.scenario, modes=args.modes))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if all(row["schema_valid"] for row in result["checks"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
