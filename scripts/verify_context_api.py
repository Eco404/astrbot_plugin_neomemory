"""Opt-in synthetic replay through real AstrBot providers and durable jobs.

Runs three extraction windows and two rolling-summary windows at most. Only
safe aggregate evidence is printed. Optional local evidence contains only
synthetic inputs and outputs; credentials and private conversations are excluded.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from verify_stage4_api import credentials, failure
from verify_stage5_api import safe_job, wait_terminal


async def run_checks(secrets: dict, directory: Path, *, only="all", http_budget=6) -> dict:
    from astrbot import __version__ as host_version
    from astrbot.api.provider import ProviderRequest
    from astrbot.core.agent.message import Message, dump_messages_with_checkpoints
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
    from astrbot.core.star.context import Context

    from adapters.runtime import MemoryRuntime
    from core.config import NeoMemoryConfig
    from core.identity import EventIdentity
    from core.maintenance import MaintenanceEngine
    from core.memory_spaces import private_space
    from core.memory_text import render_memory_text
    from core.short_term import ShortTermService
    from core.source_ingest import CapturedSource
    from storage.memory_store import MemoryStore
    from storage.source_store import SourceStore

    settings = secrets["ecoapi"]
    llm = ProviderOpenAIOfficial({"id": "isolated-llm", "type": "openai_chat_completion",
        "model": settings["model"], "api_base": settings["base_url"], "key": [settings["api_token"]],
        "timeout": 60}, {})
    # Keep the total remote spend bounded even if the host retries internally.
    llm.client.max_retries = 0
    request_count = 0

    async def count_request(_request):
        nonlocal request_count
        if request_count >= http_budget:
            raise RuntimeError("synthetic_replay_http_budget_exceeded")
        request_count += 1

    llm.client._client.event_hooks["request"].append(count_request)
    providers = {"isolated-llm": llm}

    async def get_provider_by_id(provider_id):
        return providers.get(provider_id)

    host = object.__new__(Context)
    host.provider_manager = SimpleNamespace(inst_map=providers, provider_insts=[llm],
        embedding_provider_insts=[], get_provider_by_id=get_provider_by_id)
    config = NeoMemoryConfig(llm_provider_id="isolated-llm", embedding_provider_id="",
        automatic_extraction_enabled=False, private_min_messages=3, private_topic_messages=3,
        private_max_messages=5, job_concurrency=1, job_poll_seconds=.05, job_max_attempts=1,
        extraction_timeout_seconds=90, short_term_keep_messages=1, short_term_min_messages=3,
        short_term_max_messages=4, short_term_summary_chars=400)
    sources = SourceStore(directory / "source.db")
    memory = MemoryStore(directory / "memory.db", source_exists=sources.has_source)
    plugin = SimpleNamespace(context=host, config=config, data_dir=directory, source_store=sources,
        memory_store=memory, maintenance=None, short_term=None, ready=True)
    runtime = MemoryRuntime(plugin)
    persona = "合成测试人格：温和而克制的陪伴者，关心用户持续学习与生活变化，只记录有证据的内容。"
    async def persona_loader(_persona_id):
        return persona

    engine = MaintenanceEngine(sources, memory, runtime, config, persona_loader)
    short = ShortTermService(sources, memory, engine.jobs, runtime.adapter, config, persona_loader)
    engine.runner.handlers["short_term"] = short.run
    plugin.maintenance = engine
    result = {"tested_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
              "host_version": host_version, "llm_model": settings["model"],
              "host_provider_class": type(llm).__name__, "synthetic_only": True,
              "plugin_attempts": 1, "sdk_retries": 0, "http_budget": http_budget, "only": only, "checks": []}
    sequence = 0
    identity = None
    space = None

    async def capture(texts, conversation):
        nonlocal sequence, identity, space
        records = []
        for text in texts:
            sequence += 1
            identity = EventIdentity("isolated-platform", "isolated", "FriendMessage",
                "isolated:FriendMessage:synthetic-user", "synthetic-user", "", "synthetic-user",
                "Synthetic", "synthetic-agent", f"synthetic-{sequence}")
            record = await sources.capture(CapturedSource(identity=identity, actor_kind="user",
                content_text=text, observed_at=time.time() - 500 + sequence,
                actor_account_id="synthetic-user", persona_id="synthetic_persona",
                metadata={"host_conversation_id": conversation}))
            records.append(record)
        if space is None:
            space = await memory.get_or_create_space(private_space("synthetic_persona", records[0].logical_user_id))
        return records

    async def extract(name, texts):
        await capture(texts, "long-term-case")
        start, before = time.perf_counter(), request_count
        try:
            queued = await engine.enqueue("extract", space["space_id"], name)
            job, _ = await wait_terminal(engine, kind="extract", job_id=queued["job_id"])
            check = {"name": name, "structure_ok": job["state"] == "succeeded", "job": safe_job(job)}
        except Exception as exc:
            check = {"name": name, "structure_ok": False, **failure(exc)}
        check.update(seconds=round(time.perf_counter() - start, 4), http_requests=request_count - before)
        result["checks"].append(check)
        return check

    async def summary(name, texts):
        await capture(texts, "continuity-case")
        start, before = time.perf_counter(), request_count
        try:
            queued = await short.maybe_enqueue(space["space_id"], "continuity-case", stream_id=identity.stream_id)
            if queued is None:
                raise ValueError("synthetic_summary_not_admitted")
            job, _ = await wait_terminal(engine, kind="short_term", job_id=queued["job_id"])
            state = await short.get_current(space["space_id"], "continuity-case", stream_id=identity.stream_id)
            check = {"name": name, "structure_ok": job["state"] == "succeeded", "job": safe_job(job),
                     "summary_characters": len(state["summary_text"]) if state else 0}
            (directory / f"{name}.json").write_text(json.dumps({"new_messages": texts,
                "state": state, "raw": job.get("checkpoint", {}).get("raw")}, ensure_ascii=False, indent=2))
        except Exception as exc:
            state = None
            check = {"name": name, "structure_ok": False, **failure(exc)}
        check.update(seconds=round(time.perf_counter() - start, 4), http_requests=request_count - before)
        result["checks"].append(check)
        return check, state

    def finish(check, assertions):
        # A transport/structure failure offers no evidence about semantic
        # accuracy. Do not report absence of artifacts as a model fact error.
        check["assertions"] = assertions if check["structure_ok"] else {}
        check["semantic_status"] = ("passed" if all(assertions.values()) else "failed") \
            if check["structure_ok"] else "not_evaluated"
        check["ok"] = bool(check["structure_ok"] and all(assertions.values()))

    try:
        await sources.open()
        await memory.open()
        await runtime.start()
        await engine.start()
        profiles = []
        final_state = None
        if only != "short_term":
            initial = await extract("initial_profile_and_event", [
                "过去十年我每天都喝两杯黑咖啡，这是我长期的习惯。",
                "我刚报名一门为期半年的摄影课程，已经缴费。",
                "课程2026年11月7日开课，每周六上午上课。",
                "学摄影是为了给家人拍旅行照片，我打算坚持上完。",
                "我信任你，希望你记住我的摄影学习进展，之后可以继续聊。",
            ])
            nodes = await memory.list_nodes(space_id=space["space_id"])
            profiles = await memory.list_profiles(space_id=space["space_id"], logical_user_id=space["logical_user_id"])
            profile_text = " ".join(item["description"] + " ".join(item["tags"]) for item in profiles)
            event_ids = {node["node_id"] for node in nodes if node["node_type"] == "event" and "摄影" in node["content_text"]}
            finish(initial, {"profile_mentions_coffee": "咖啡" in profile_text,
                             "photography_event_created": bool(event_ids), "nonempty_memories": bool(nodes)})
            if initial["structure_ok"]:
                updated = await extract("preference_correction", [
                    "从今天开始我不再喝咖啡，改为每天只喝乌龙茶。",
                    "这是我打算长期保持的新习惯，请更新你对我的了解。",
                    "摄影课程的安排没有变化，我还会继续上课。",
                ])
                nodes = await memory.list_nodes(space_id=space["space_id"])
                profiles = await memory.list_profiles(space_id=space["space_id"], logical_user_id=space["logical_user_id"])
                profile_text = " ".join(item["description"] + " ".join(item["tags"]) for item in profiles)
                memories = [node["content_text"] for node in nodes]
                finish(updated, {"profile_mentions_current_tea": "乌龙茶" in profile_text,
                                 "profile_no_old_daily_coffee_claim": not bool(re.search(r"每天(?:都)?(?:喝)?两杯(?:黑)?咖啡", profile_text)),
                                 "long_term_retains_change_history": any("咖啡" in text and "茶" in text for text in memories)})
                if updated["structure_ok"]:
                    continued = await extract("same_event_continuation", [
                        "之前报名的那门半年摄影课程已经结业，今天是2027年5月8日。",
                        "我坚持每周六去上课，顺利拿到了那门课程的结业证书。",
                        "这就是之前告诉你的2026年11月7日开始的摄影课程，没有报名另一门。",
                    ])
                    nodes = await memory.list_nodes(space_id=space["space_id"])
                    course_nodes = [node for node in nodes if node["node_type"] == "event" and "摄影" in node["content_text"]]
                    finish(continued, {"existing_event_identity_updated": any(node["node_id"] in event_ids
                        and node["content_version"] > 1 and "结业" in node["content_text"] for node in course_nodes),
                        "no_duplicate_photography_event": len(course_nodes) == 1})

        if only != "extraction":
            first, first_state = await summary("rolling_summary_initial", [
                "下周一有面试，我正在准备自我介绍。",
                "自我介绍还没有写完，今晚先完成初稿。",
                "明天16点再做模拟面试，具体问题还没列好。",
                "请记住先完成自我介绍，再准备模拟面试问题。",
                "好的。",
            ])
            text = first_state["summary_text"] if first_state else ""
            finish(first, {"self_introduction_present": "自我介绍" in text,
                           "mock_interview_present": "模拟面试" in text,
                           "latest_raw_message_reserved": bool(first_state and first_state["coverage"]["source_count"] == 4)})
            final_state = first_state
            if first["structure_ok"]:
                second, final_state = await summary("rolling_summary_correction", [
                    "自我介绍初稿已经完成，不用再提醒我写了。",
                    "模拟面试改到后天上午10点，明天16点的计划取消。",
                    "现在只差练习项目经历的表达，这项还没完成。",
                    "我们继续聊。",
                ])
                text = final_state["summary_text"] if final_state else ""
                finish(second, {"new_interview_time_present": "后天" in text and bool(re.search(r"10|十", text)),
                    "remaining_project_practice_present": "项目经历" in text,
                    "old_unfinished_introduction_not_repeated": not bool(re.search(r"自我介绍.{0,10}(还没|尚未|未完成)", text)),
                    "same_summary_updated": bool(first_state and final_state and first_state["state_id"] == final_state["state_id"]
                        and final_state["content_version"] == first_state["content_version"] + 1),
                    "only_new_window_added": bool(final_state and final_state["coverage"]["source_count"] == 8)})

        class SyntheticEvent:
            unified_msg_origin = identity.unified_msg_origin
            message_obj = SimpleNamespace(message_id="front-request")

            def get_platform_id(self):
                return identity.platform_instance_id

            def get_platform_name(self):
                return identity.platform_name

            def get_message_type(self):
                return identity.message_type

            def get_session_id(self):
                return identity.session_id

            def get_group_id(self):
                return ""

            def get_sender_id(self):
                return identity.account_id

            def get_sender_name(self):
                return "Synthetic"

            def get_self_id(self):
                return identity.self_id

            def get_message_str(self):
                return "接下来该准备什么？"

        async def resolve_persona(event, *, conversation=None):
            event._neomemory_conversation_id = getattr(conversation, "cid", None)
            return "synthetic_persona"

        plugin._resolve_persona_id = resolve_persona
        request = ProviderRequest(prompt="接下来该准备什么？")
        request.conversation = SimpleNamespace(cid="continuity-case" if only != "extraction" else "long-term-case")
        before = request_count
        start = time.perf_counter()
        await runtime.passive(SyntheticEvent(), request)
        injected = request._neomemory_part.text if getattr(request, "_neomemory_part", None) else ""
        persisted = json.dumps(dump_messages_with_checkpoints([Message.model_validate(await request.assemble_context())]),
                               ensure_ascii=False)
        assertions = {"nonempty_local_context": bool(injected),
            "no_foreground_model_call": request_count == before,
            "within_shared_budget": len(injected.encode()) <= config.recall_budget_tokens,
            "temporary_part": bool(getattr(request, "_neomemory_part", None) and request._neomemory_part._no_save),
            "absent_from_host_history": "NeoMemory 补充资料" not in persisted,
            "real_model_context_present": bool(final_state and render_memory_text(
                final_state["summary_text"], {space["logical_user_id"]: "Synthetic"}) in injected)
                if only != "extraction" else bool(profiles and "用户画像" in injected)}
        result["checks"].append({"name": "frontend_context_from_real_outputs", "ok": all(assertions.values()),
            "seconds": round(time.perf_counter() - start, 4), "http_requests": request_count - before,
            "assertions": assertions, "diagnostics": runtime.diagnostics.get("last_context", {})})
    finally:
        result["http_requests_total"] = request_count
        await engine.stop()
        await runtime.close()
        await memory.close()
        await sources.close()
        await llm.terminate()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    parser.add_argument("--only", choices=("all", "extraction", "short_term"), default="all")
    parser.add_argument("--http-budget", type=int, choices=range(1, 7), default=6)
    parser.add_argument("--output", type=Path, help="保留独立合成证据的新目录；省略则使用临时目录")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        if args.output:
            args.output.mkdir(parents=True, exist_ok=False)
        with (contextlib.nullcontext(str(args.output)) if args.output else
              tempfile.TemporaryDirectory(prefix="neomemory-context-api-")) as temporary:
            directory = Path(temporary)
            (directory / "data/config").mkdir(parents=True)
            os.environ["ASTRBOT_ROOT"] = str(directory)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                from loguru import logger
                logger.remove()
                result = asyncio.run(run_checks(credentials(args.credentials), directory,
                    only=args.only, http_budget=args.http_budget))
            if args.output:
                (directory / "report.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as exc:
        result = {"setup_or_run": failure(exc)}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    checks = result.get("checks", [])
    expected = {"all": 6, "extraction": 4, "short_term": 3}[args.only]
    return 0 if len(checks) == expected and all(check["ok"] for check in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
