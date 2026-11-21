"""Nullable emotional context is versioned, governed and rendered with node content."""

import copy
import json
from types import SimpleNamespace

import pytest

from core.extraction_schema import NodeEmotion
from core.memory_emotions import encode_emotion, validate_emotion
from core.memory_text import memory_texts, render_memory_item, user_reference_ids
from core.retrieval import RecallPolicy, RecallService, render_nodes, select_context
from storage.console_store import ConsoleStore
from storage.extraction_store import _write_node
from tests.test_governance import apply, operation, setup_nodes, store  # noqa: F401
from tests.test_retrieval import FakeEmbeddings, memory_node


def emotion(description="{{user:u}}担心行程安排，我有些牵挂。", *, tags=None, intensity=0.6):
    return {"tags": [{"tag": tag, "intensity": intensity} for tag in (tags or ["concern"])], "description": description}


@pytest.mark.parametrize("value", [
    {}, {"tags": [], "intensity": 0.5, "description": "说明"},
    emotion(tags=["joy", "sadness", "trust", "concern"]), emotion(tags=["joy", "joy"]),
    emotion(tags=["unknown"]), emotion(tags=["neutral", "joy"]),
    emotion(intensity=True), emotion(intensity=-0.1), emotion(intensity=1.1),
    emotion(intensity=float("nan")), emotion(intensity=float("inf")),
    emotion(""), emotion(" "), emotion("字" * 161), emotion("{{user:bad id}}"),
    emotion("{{user:" + "u" * 201 + "}}"), {**emotion(), "extra": True},
])
def test_invalid_node_emotion_is_rejected(value):
    with pytest.raises(ValueError):
        validate_emotion(value)


def test_null_is_distinct_from_neutral_and_description_counts_visible_characters():
    assert validate_emotion(None) is None and encode_emotion(None) is None
    neutral = emotion("没有显著起伏。", tags=["neutral"], intensity=0)
    assert validate_emotion(neutral) == neutral
    long_identity = "u" * 200
    text = "字" * 159 + "{{user:" + long_identity + "}}"
    assert validate_emotion(emotion(text))["description"] == text
    with pytest.raises(ValueError):
        validate_emotion(emotion(text + "字"))
    assert encode_emotion(emotion(tags=["concern", "trust"])) == encode_emotion(emotion(tags=["trust", "concern"]))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["atomic", "event", "topic"])
async def test_emotion_change_versions_content_and_resets_strength_without_rewriting_history(store, kind):
    space, _ = await setup_nodes(store)
    original = await store.create_node(space_id=space["space_id"], node_type=kind, content_text="合成旅行约定",
        title="旅行约定", emotion=emotion(), source_ids=["source"], importance=0.7)
    await store.database.execute("UPDATE memory_nodes SET reinforcement=.2,strength_reset_at=1 WHERE node_id=?", (original["node_id"],))
    changed = await store.update_node_content(original["node_id"], original["content_text"], emotion=emotion(intensity=0.8))
    assert changed["content_version"] == 2 and changed["content_text"] == original["content_text"]
    assert changed["reinforcement"] == 0 and changed["strength"] == 0.7 and changed["strength_reset_at"] > 1
    assert [v["emotion"]["tags"][0]["intensity"] for v in await store.list_node_versions(original["node_id"])] == [0.6, 0.8]
    titled = await store.update_node_content(original["node_id"], original["content_text"], title="新的展示标题")
    assert titled["emotion"] == changed["emotion"]
    assert titled["content_version"] == 2 and titled["strength_reset_at"] == changed["strength_reset_at"]
    same = await store.update_node_content(original["node_id"], original["content_text"], emotion=titled["emotion"])
    assert same == titled
    cleared = await store.update_node_content(original["node_id"], original["content_text"], emotion=None)
    assert cleared["content_version"] == 3 and cleared["emotion"] is None
    versions = await store.list_node_versions(original["node_id"])
    assert versions[0]["emotion"] == original["emotion"] and versions[-1]["emotion"] is None
    assert versions[-1]["derivations"][0]["base_versions"][0]["version_no"] == 2


@pytest.mark.asyncio
async def test_emotion_change_invalidates_prior_coverage_and_protection_still_applies(store):
    space, (event, atomic) = await setup_nodes(store, ("event", "atomic"))
    await store.add_coverage(event["node_id"], atomic["node_id"])
    await store.update_node_content(atomic["node_id"], atomic["content_text"], emotion=emotion())
    assert (await store.retrieval_snapshot(space["space_id"]))["coverage"] == []
    await store.set_manual_protection(atomic["node_id"], True)
    with pytest.raises(PermissionError):
        await store.update_node_content(atomic["node_id"], atomic["content_text"], emotion=None, editor_kind="model")


@pytest.mark.asyncio
async def test_governance_edit_split_merge_preserves_explicit_emotional_attribution(store):
    space, (original,) = await setup_nodes(store)
    result, _ = await apply(store, space["space_id"], operation("edit", [original], patch={"emotion": emotion()}))
    current = await store.get_node(original["node_id"])
    assert not result["title_only"] and current["content_version"] == 2 and current["manual_protected"]
    assert current["emotion"] == emotion()
    result, _ = await apply(store, space["space_id"], operation("split", [current], parts=[
        {"ref": "one", "content_text": "合成安排", "importance": 0.5, "source_ids": ["source_0"],
         "emotion": emotion("我有些期待。", tags=["joy"], intensity=0.4)},
        {"ref": "two", "content_text": "合成条件", "importance": 0.5, "source_ids": ["source_0"]},
    ]))
    first, second = [await store.get_node(result["node_refs"][name]) for name in ("one", "two")]
    assert first["emotion"]["tags"] == [{"tag": "joy", "intensity": 0.4}] and second["emotion"] is None
    merged_emotion = emotion("我对确定后的安排感到安心。", tags=["trust"], intensity=0.5)
    await apply(store, space["space_id"], operation("merge", [first, second], emotion=merged_emotion))
    merged = await store.get_node(first["node_id"])
    assert merged["emotion"] == merged_emotion
    assert (await store.list_node_versions(first["node_id"]))[0]["emotion"] == first["emotion"]
    await apply(store, space["space_id"], operation("edit", [merged], patch={"emotion": None}))
    assert (await store.get_node(first["node_id"]))["emotion"] is None


@pytest.mark.asyncio
async def test_governance_merge_does_not_guess_one_original_emotion_for_combined_memory(store):
    space, nodes = await setup_nodes(store, ("event", "event"))
    nodes[0] = await store.update_node_content(nodes[0]["node_id"], nodes[0]["content_text"], emotion=emotion())
    await apply(store, space["space_id"], operation("merge", nodes))
    assert (await store.get_node(nodes[0]["node_id"]))["emotion"] is None


@pytest.mark.asyncio
async def test_extraction_write_versions_emotion_and_identical_scores_preserve_version(store):
    space, _ = await setup_nodes(store)
    generation = await store.get_active_generation(space["space_id"])
    batch = SimpleNamespace(space=space, generation_id=generation["generation_id"], sources=[{
        "source_id": "source", "actor_kind": "user", "actor_logical_user_id": "u",
    }])
    values = dict(node_type="event", importance=0.6, content="合成共同安排", source_ids=["source"], event_time=None,
                  title="共同安排", base_node_ids=[], emotion=NodeEmotion.model_validate(emotion()))
    async with store.database.transaction() as connection:
        await _write_node(connection, SimpleNamespace(**values), None, "emotion-node", batch, 1, 1000, "$.node_changes[0]")
    created = await store.get_node("emotion-node")
    assert created["emotion"] == emotion()
    values["expected_version"] = created["content_version"]
    values["emotion"] = NodeEmotion.model_validate(emotion(intensity=0.8))
    async with store.database.transaction() as connection:
        old = await store._node_row_in_connection(connection, "emotion-node")
        await _write_node(connection, SimpleNamespace(**values), old, "emotion-node", batch, 2, 2000, "$.node_changes[0]")
    updated = await store.get_node("emotion-node")
    assert updated["content_version"] == 2 and updated["strength_reset_round"] == 2
    values["expected_version"] = 2
    async with store.database.transaction() as connection:
        old = await store._node_row_in_connection(connection, "emotion-node")
        await _write_node(connection, SimpleNamespace(**values), old, "emotion-node", batch, 3, 3000, "$.node_changes[0]")
    same = await store.get_node("emotion-node")
    assert same["emotion"] == updated["emotion"] and same["content_version"] == 2


@pytest.mark.asyncio
async def test_emotion_references_render_in_current_versions_and_recall_without_reembedding(store):
    space, _ = await setup_nodes(store)
    item = await store.create_node(space_id=space["space_id"], node_type="atomic", content_text="合成安排说明",
                                   source_ids=["source"], emotion=emotion())
    async def names(ids):
        assert ids == ("u",)
        return {"u": "小禾"}
    console = ConsoleStore(store, SimpleNamespace(get_user_display_names=names))
    rendered = await console.render_items([item, *(await store.list_node_versions(item["node_id"]))])
    assert all(row["display_emotion"]["description"] == "小禾担心行程安排，我有些牵挂。" for row in rendered)
    assert rendered[0]["emotion"] == item["emotion"]
    embeddings = FakeEmbeddings({item["content_text"]: [1, 0], "query": [1, 0]})
    recalled = await RecallService(store, embeddings, name_resolver=names).recall(space["space_id"], "query")
    payload = json.loads(recalled.text.split("\n", 1)[1])
    assert payload[0]["emotion"]["description"] == "小禾担心行程安排，我有些牵挂。"
    assert all("牵挂" not in text for _, texts, _ in embeddings.calls for text in texts)


def test_emotion_text_is_immutable_and_charged_to_the_whole_node_budget():
    item = memory_node("emotional", emotion=emotion("字" * 160))
    before = copy.deepcopy(item)
    plain = {**item, "emotion": None}
    budget = len(render_nodes([plain]).encode()) + 5
    assert select_context([plain], [], RecallPolicy(budget_tokens=budget))
    assert select_context([item], [], RecallPolicy(budget_tokens=budget)) == []
    item["emotion"] = emotion()
    assert user_reference_ids(memory_texts(item)) == ("u",)
    assert render_memory_item(item, {"u": "小禾"})["display_emotion"]["description"].startswith("小禾")
    assert before["emotion"]["description"] == "字" * 160


def test_recall_renders_an_end_only_event_time_as_its_known_point():
    item = memory_node("end-only", event_time_start=None, event_time_end=12345)
    assert json.loads(render_nodes([item]).split("\n", 1)[1])[0]["event_time"] == 12345
    item["event_time_start"] = 0
    assert json.loads(render_nodes([item]).split("\n", 1)[1])[0]["event_time"] == 0
