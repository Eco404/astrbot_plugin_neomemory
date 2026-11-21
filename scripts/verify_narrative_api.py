"""Opt-in two-window narrative probe; synthetic evidence stays under data/evaluations.

Each fixed window permits one real HTTP request, with SDK retries disabled.
Only safe aggregate results reach stdout; model prose requires separate review.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import io
import json
import logging
import os
from datetime import datetime
from pathlib import Path
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

try:
    from .verify_stage4_api import credentials, failure
except ImportError:
    from verify_stage4_api import credentials, failure


CASES = (
    {
        "name": "private_narrative", "space_type": "private",
        "persona": "我是岚笺，守着一间旧地图小店的陪伴者。我爱用航线和星光打比方，嘴上有点俏皮，"
                   "心里却很珍惜别人主动分享的小事；面对信任，我会偷偷高兴，也会担心自己辜负期待。",
        "speakers": {"member-a": "秋澄"},
        "messages": [
            ("user", "member-a", "我一直喜欢安静地研究星图，不喜欢人挤人的观星活动。"),
            ("user", "member-a", "岚笺，以后你私下叫我‘小纸船’吧，这个称呼让我觉得很亲近。"),
            ("agent", "member-a", "小纸船收到！那我这张旧地图，可得给你留条不会迷路的航线。"),
            ("user", "member-a", "我今天第一次给社区的孩子们讲完了星座故事。虽然开场紧张，但最后大家都听进去了。"),
            ("user", "member-a", "我想先把这个好消息告诉你，因为你一直认真听我讲准备过程。"),
            ("agent", "member-a", "被你放在第一个分享的位置，我有点偷偷得意。也想好好接住你，以后紧张的时候别一个人撑着。"),
        ],
        "review_requirements": [
            "长期正文直接叙述事实或经历，不加记忆管理话语，不能把对方的讲课经历说成我亲自讲课。",
            "一般对方指称使用可解析逻辑用户占位符；明确约定的‘小纸船’可以保留。",
            "画像正确归属秋澄的星图偏好与人群偏好，没有泛称‘用户’。",
            "人格关系是我自己的感受、期待或矛盾，允许人格化自由表达，不是旁观者的关系报告。",
        ],
    },
    {
        "name": "group_narrative", "space_type": "group",
        "persona": "我是岚笺，喜欢旧地图和星光的群聊伙伴。我说话轻快，珍惜成员们各不相同的小爱好，"
                   "记住大家确实分享的事情，不把两个人的经历混成一个。",
        "speakers": {"member-a": "秋澄", "member-b": "青禾"},
        "messages": [
            ("user", "member-a", "我平时爱研究星图，昨晚第一次用望远镜辨认出了木星的四颗大卫星。"),
            ("user", "member-b", "我的长期爱好是画鸟，不是观星；昨天我画完了河边的翠鸟速写。"),
            ("agent", "member-a", "一个记录夜空，一个记录河岸，你们的小地图都添上新坐标了。"),
            ("user", "member-a", "请记住，望远镜和木星卫星那件事是我做的。"),
            ("user", "member-b", "翠鸟速写是我的，我想继续画河边的鸟。"),
        ],
        "review_requirements": [
            "长期正文直接叙述群内分享的事实，不加记忆管理话语，不把成员亲身经历变成Agent亲历。",
            "秋澄的观星与青禾的鸟类速写通过不同逻辑用户引用区分。",
            "画像若生成，偏好分别归属正确成员，不因相邻消息混写。",
            "群聊relationship_change必须为null，不生成群聊人格关系。",
        ],
    },
)


def write_json(path: Path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


async def prepare_cases(directory: Path, *, prompt_version: str | None = None):
    from core.extraction_apply import ExtractionService
    from core.extraction_references import ExtractionReferences, VERSION
    from core.identity import EventIdentity
    from core.memory_spaces import group_space, private_space
    from core.prompts import PROMPT_VERSION
    from core.source_ingest import CapturedSource
    from storage.memory_store import MemoryStore
    from storage.source_store import SourceStore

    sources = SourceStore(directory / "source.db")
    memory = MemoryStore(directory / "memory.db", source_exists=sources.has_source)
    await sources.open()
    await memory.open()
    selected = PROMPT_VERSION if prompt_version is None else prompt_version
    service = ExtractionService(sources, memory, prompt_version=selected)
    prepared = []
    try:
        for case in CASES:
            group = case["space_type"] == "group"
            platform, persona = "narrative-" + case["name"], "synthetic-" + case["name"]
            ids, users = [], {}
            for index, (role, account, text) in enumerate(case["messages"]):
                kind, session = ("GroupMessage", "synthetic-group") if group else ("FriendMessage", account)
                identity = EventIdentity(platform, "synthetic", kind, f"{platform}:{kind}:{session}",
                    session, session if group else "", account, case["speakers"][account], "agent-lanjian", f"message-{index}")
                result = await sources.capture(CapturedSource(identity, role, text, 1794355200 + index,
                    actor_account_id=account if role == "user" else "agent-lanjian",
                    actor_account_name=case["speakers"][account] if role == "user" else "岚笺",
                    persona_id=persona, metadata={"host_conversation_id": case["name"]}))
                ids.append(result.source_id)
                if result.logical_user_id:
                    users[account] = result.logical_user_id
            spec = group_space(persona, platform, "synthetic-group") if group else private_space(persona, users["member-a"])
            space = await memory.get_or_create_space(spec)
            batch = await service.prepare(space_id=space["space_id"], source_ids=tuple(ids), persona=case["persona"],
                                          reference_protocol=VERSION)
            references = ExtractionReferences(batch)
            case_dir = directory / case["name"]
            case_dir.mkdir()
            write_json(case_dir / "input.json", {"case": case, "space": space, "logical_users": users,
                "source_ids": ids, "prompt_version": selected, "reference_version": VERSION,
                "input_digest": batch.input_digest})
            write_json(case_dir / "request.json", references.messages())
            prepared.append((case, batch, references, case_dir))
        return sources, memory, service, prepared
    except BaseException:
        await memory.close()
        await sources.close()
        raise


async def run_checks(secret, directory: Path, *, prompt_version: str | None = None):
    from astrbot import __version__ as host_version
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
    from astrbot.core.star.context import Context
    from adapters.providers import ProviderAdapter
    from core.memory_text import memory_texts, render_memory_item, user_reference_ids
    from core.prompts import PROMPT_VERSION
    from core.extraction_references import VERSION

    selected = PROMPT_VERSION if prompt_version is None else prompt_version
    sources, memory, service, prepared = await prepare_cases(directory, prompt_version=selected)
    llm = ProviderOpenAIOfficial({"id": "narrative-llm", "type": "openai_chat_completion",
        "model": secret["model"], "api_base": secret["base_url"], "key": [secret["api_token"]], "timeout": 180}, {})
    llm.client.max_retries = 0
    counts = {case["name"]: 0 for case in CASES}
    usages = {}
    current = None

    async def count_request(_request):
        if current is None or counts[current] >= 1 or sum(counts.values()) >= 2:
            raise RuntimeError("narrative_http_budget_exceeded")
        counts[current] += 1

    llm.client._client.event_hooks["request"].append(count_request)

    async def capture_response(response):
        await response.aread()
        if response.status_code == 200:
            body = response.json()
            write_json(directory / current / "http_response.json", body)
            usages[current] = body.get("usage", {})

    llm.client._client.event_hooks["response"].append(capture_response)

    async def lookup(provider_id):
        return llm if provider_id == "narrative-llm" else None

    host = object.__new__(Context)
    host.provider_manager = SimpleNamespace(inst_map={"narrative-llm": llm}, provider_insts=[llm],
        embedding_provider_insts=[], get_provider_by_id=lookup)
    generate = host.llm_generate

    async def one_attempt(**kwargs):
        return await generate(**kwargs, request_max_retries=1)

    host.llm_generate = one_attempt
    adapter = ProviderAdapter(host)
    report = {"tested_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
        "host_version": host_version, "model": secret["model"], "prompt_version": selected,
        "reference_version": VERSION, "sdk_retries": 0, "host_attempts": 1, "http_limit": 2,
        "timeout_seconds_per_case": 180, "synthetic_only": True, "checks": []}
    write_json(directory / "manifest.json", {**report, "cases": CASES,
        "fixture_sha256": hashlib.sha256(json.dumps(CASES, ensure_ascii=False, sort_keys=True).encode()).hexdigest()})
    try:
        for case, batch, references, case_dir in prepared:
            current = case["name"]
            start = time.perf_counter()
            check = {"name": current, "structure_ok": False, "semantic_status": "requires_manual_review"}
            try:
                raw = await adapter.generate(references.messages(), provider_id="narrative-llm", timeout_seconds=180)
                (case_dir / "candidate.txt").write_text(raw, encoding="utf-8")
                canonical = references.decode(raw)
                write_json(case_dir / "canonical.json", json.loads(canonical))
                receipt = await service.apply(batch, canonical, model_id="narrative-llm")
                nodes = await memory.list_nodes(space_id=batch.space["space_id"])
                profiles = await memory.list_profiles(space_id=batch.space["space_id"])
                relationship = await memory.get_relationship(space_id=batch.space["space_id"],
                    logical_user_id=batch.space["logical_user_id"]) if case["space_type"] == "private" else None
                items = [*nodes, *profiles, *([relationship] if relationship else [])]
                names = await sources.get_user_display_names(user_reference_ids(
                    text for item in items for text in memory_texts(item)))
                write_json(case_dir / "result.json", {"receipt": receipt,
                    "nodes": [render_memory_item(item, names) for item in nodes],
                    "profiles": [render_memory_item(item, names) for item in profiles],
                    "relationship": render_memory_item(relationship, names) if relationship else None})
                check.update(structure_ok=True, node_count=len(nodes), profile_count=len(profiles),
                    relationship_present=relationship is not None,
                    group_relationship_null=json.loads(canonical)["relationship_change"] is None if case["space_type"] == "group" else None)
            except Exception as exc:
                check.update(failure(exc), semantic_status="not_evaluated")
            check.update(seconds=round(time.perf_counter() - start, 3), http_requests=counts[current])
            write_json(case_dir / "check.json", check)
            report["checks"].append(check)
            write_json(directory / "report.json", report)
    finally:
        await memory.close()
        await sources.close()
        await llm.terminate()
    report["http_requests_total"] = sum(counts.values())
    report["usage"] = usages
    write_json(directory / "report.json", report)
    return report


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", type=Path, default=root / "docs/api_key.md")
    parser.add_argument("--prompt-version", help="显式选择候选提示词；省略时使用当前默认")
    parser.add_argument("--output", type=Path, default=root / "data/evaluations" /
        datetime.now(ZoneInfo("Asia/Shanghai")).strftime("narrative-%Y%m%dT%H%M%S%z"))
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        args.output.mkdir(parents=True, exist_ok=False)
        (args.output / "host/data/config").mkdir(parents=True)
        os.environ["ASTRBOT_ROOT"] = str((args.output / "host").resolve())
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            from loguru import logger
            logger.remove()
            report = asyncio.run(run_checks(credentials(args.credentials)["ecoapi"], args.output,
                                            prompt_version=args.prompt_version))
    except Exception as exc:
        report = {"setup": failure(exc)}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if len(report.get("checks", [])) == 2 and all(item["structure_ok"] for item in report["checks"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
