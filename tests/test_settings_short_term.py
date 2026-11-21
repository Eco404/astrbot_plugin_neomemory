"""A dedicated summary provider works even without a long-term LLM configured."""

import json
from types import SimpleNamespace

import pytest

from core.config import NeoMemoryConfig
from core.identity import EventIdentity
from core.memory_spaces import private_space
from core.short_term import ShortTermService
from core.short_term_prompts import PROMPT_VERSION
from core.source_ingest import CapturedSource
from storage.job_store import JobStore
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore


@pytest.mark.asyncio
async def test_dedicated_provider_handles_real_short_term_window_with_long_term_unconfigured(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await sources.open()
    await memory.open()
    fingerprints, calls = [], []
    def fingerprint(provider_id, revision=""):
        fingerprints.append(provider_id)
        return provider_id
    async def generate(messages, *, provider_id, timeout_seconds, diagnostics):
        calls.append(provider_id)
        diagnostics.update(finish_reason="stop")
        prompt = json.loads(messages[-1]["content"])
        return json.dumps({"schema_version": PROMPT_VERSION, "summary_text": "用户正在准备下周的面试。",
                           "evidence_source_ids": [row["source_id"] for row in prompt["new_sources"]]})
    config = NeoMemoryConfig.from_mapping({"short_term_provider_id": "summary-only", "short_term_keep_messages": 2,
                                           "short_term_min_messages": 3, "short_term_max_messages": 4})
    adapter = SimpleNamespace(llm_fingerprint=fingerprint, generate=generate)
    jobs = JobStore(memory.database)
    service = ShortTermService(sources, memory, jobs, adapter, config, lambda _: "认真温和的朋友")
    try:
        records = []
        for index in range(8):
            identity = EventIdentity("platform", "test", "FriendMessage", "platform:u", "u",
                                     "", "u", "user", "bot", f"chat:{index}")
            records.append(await sources.capture(CapturedSource(identity=identity, actor_kind="user",
                content_text="我正在准备下周的面试", observed_at=100 + index, actor_account_id="u", persona_id="p",
                metadata={"host_conversation_id": "chat"})))
        space = await memory.get_or_create_space(private_space("p", records[0].logical_user_id))
        job = await service.maybe_enqueue(space["space_id"], "chat", stream_id=records[0].stream_id)
        assert job is not None
        claimed = await jobs.claim("worker")
        await service.run(claimed)
        summary = await service.get_current(space["space_id"], "chat", stream_id=records[0].stream_id)
        assert summary["summary_text"] == "用户正在准备下周的面试。"
        assert calls == ["summary-only"] and set(fingerprints) == {"summary-only"}
    finally:
        await memory.close()
        await sources.close()
