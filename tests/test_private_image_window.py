"""Commit, bounded correction, degradation, and replay of a real private image window."""

import json
from dataclasses import replace

import pytest

from adapters.providers import VisionUnsupportedError
from core.image_cache import PrivateImageCache
from tests.test_maintenance_engine import environment, execute, enqueue_and_claim, node, output, restart


async def prepare(environment, tmp_path, slots=2):
    env = environment
    cache = PrivateImageCache(tmp_path / "vision")
    await cache.open()
    env.engine.image_cache = cache
    source = env.records[0].source_id
    metadata = {"image_attachments": [{"slot": i + 1} for i in range(slots)]}
    await env.sources.database.execute("UPDATE source_records SET metadata_json=? WHERE source_id=?",
                                       (json.dumps(metadata), source))
    cache._write(cache._path(source, 2, "jpg"), b"test JPEG payload")
    return source, cache


@pytest.mark.asyncio
async def test_image_evidence_survives_restart_rebuild_and_correction(environment, tmp_path):
    env = environment
    source, cache = await prepare(env, tmp_path)
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)
    change = output([node("s1", content="报名地点在 417 室。")])
    def answer(slot):
        return json.dumps({"extraction": change, "image_evidence": [
            {"source_id": "s1", "slot": slot, "description": "海报上写着 417 室。"}]})

    async def generate(messages, **kwargs):
        env.adapter.calls.append(messages)
        if len(env.adapter.calls) == 1:
            assert [p["type"] for p in messages[1]["content"]].count("image_url") == 1
            assert "image_unavailable" in str(messages[1]["content"])
            return answer(1)  # First attachment expired; a later attachment is readable.
        if len(env.adapter.calls) == 2:
            assert "invalid_image_evidence" in messages[-1]["content"]
            assert messages[1] == env.adapter.calls[0][1]
            return answer(2)
        assert isinstance(messages[1]["content"], str)
        assert "海报上写着 417 室" in messages[1]["content"]
        assert "data:image" not in str(messages)
        return json.dumps(change)

    env.adapter.generate = generate
    try:
        result = await execute(env, await enqueue_and_claim(env))
        assert result["state"] == "succeeded", result
        assert not list(cache.directory.iterdir())
        assert len(env.adapter.calls) == 2
        assert [(e["slot"], e["origin"]) for e in await env.sources.list_image_evidence((source,))] == [(2, "image")]
    finally:
        await cache.close()
    restart(env)
    rebuilt = await execute(env, await enqueue_and_claim(env, "rebuild", key="replay"))
    assert rebuilt["state"] == "succeeded", rebuilt
    assert len(env.adapter.calls) == 3


@pytest.mark.asyncio
async def test_unsupported_vision_finishes_window_on_text_and_releases_cache(environment, tmp_path):
    env = environment
    source, cache = await prepare(env, tmp_path)
    async def generate(messages, **kwargs):
        env.adapter.calls.append(messages)
        if len(env.adapter.calls) == 1:
            raise VisionUnsupportedError("image input unsupported")
        assert isinstance(messages[1]["content"], str)
        assert "vision_unsupported" in messages[1]["content"]
        return json.dumps(output())
    env.adapter.generate = generate
    try:
        result = await execute(env, await enqueue_and_claim(env))
        assert result["state"] == "succeeded", result
        assert not list(cache.directory.iterdir())
        assert not await env.sources.list_image_evidence((source,))
        assert len(env.adapter.calls) == 2
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_window_cap_counts_missing_positions_and_replays_previously_adopted_evidence(environment):
    from core.window_images import prepare_slots

    env = environment
    one, two = (record.source_id for record in env.records)
    records = [{"source_id": source, "metadata_json": json.dumps({
        "image_attachments": [{"slot": slot} for slot in range(1, count + 1)]})}
        for source, count in [(one, 3), (two, 8)]]
    selected = await prepare_slots(env.sources, None, records, limit=10)
    assert [(item["source_alias"], item["slot"]) for item in selected] == [
        *(("s1", i) for i in range(1, 4)), *(("s2", i) for i in range(1, 8))]
    await env.sources.save_image_evidence([
        {"source_id": two, "slot": 8, "description": "已采用的细节", "origin": "image"}])
    replay = await prepare_slots(env.sources, None, records, limit=0)
    assert [(item["source_alias"], item["slot"], item["evidence"]) for item in replay] == [
        ("s2", 8, "已采用的细节")]
