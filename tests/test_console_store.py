import asyncio
from dataclasses import replace

import pytest
import pytest_asyncio

from core.jobs import ReviewRequired
from core.memory_spaces import group_space, private_space
from storage.console_store import ConsoleStore, NoteService, validate_note_operation
from storage.job_store import JobConflict, JobStore, LeaseLost
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore
from tests.test_source_store import make_source


@pytest_asyncio.fixture
async def stores(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    memory = MemoryStore(tmp_path / "memory.db")
    await sources.open()
    await memory.open()
    result = await sources.capture(make_source("first"))
    space = await memory.get_or_create_space(private_space("persona-default", result.logical_user_id))
    jobs = JobStore(memory.database)
    try:
        yield memory, sources, space, ConsoleStore(memory, sources), NoteService(memory, jobs)
    finally:
        await memory.close()
        await sources.close()


async def make_node(stores, text="A memory", **kwargs):
    memory, sources, space, _, _ = stores
    source = (await sources.list_records())[0]
    return await memory.create_node(space_id=space["space_id"], node_type="event", content_text=text,
                                    source_ids=[source["source_id"]], **kwargs)


async def execute(service, space, key, operation):
    job = await service.enqueue(space["space_id"], key, operation)
    claimed = await service.jobs.claim("test-worker")
    assert claimed["job_id"] == job["job_id"]
    result = await service.run(claimed)
    await service.jobs.finish(claimed["job_id"], claimed["lease_token"], result=result)
    return result


@pytest.mark.asyncio
async def test_space_storage_follows_source_ownership_and_counts_stored_bytes(stores, tmp_path):
    from storage.cache_store import CacheStore, text_hash

    memory, sources, private, console, _ = stores
    sid = private["space_id"]
    group = await memory.get_or_create_space(group_space("persona-default", "adapter-1", "room"))
    gid = group["space_id"]

    async def sizes(cache=None):
        return {item["space_id"]: item["size_bytes"] for item in await console.space_storage(cache)}

    initial = await sizes()
    assert initial[sid] >= len(b"hello") and initial[gid] == 0
    message = make_source("group-msg", "群聊")
    await sources.capture(replace(message, identity=replace(message.identity, group_id="room", message_type="GroupMessage")))
    after_group = await sizes()
    assert after_group[gid] >= len("群聊".encode()) and after_group[sid] == initial[sid]
    await memory.create_note(space_id=sid, content_text="笔记")
    after_note = await sizes()
    assert after_note[sid] - initial[sid] == len("笔记{}".encode())

    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    try:
        for namespace in (gid, "unowned"):
            await cache.put_vectors("model", namespace, [text_hash("synthetic")], [[.6, .8]])
        result = await sizes(cache.path)
        assert result == {sid: after_note[sid], gid: after_group[gid] + len("[0.6,0.8]")}
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_context_current_generation_decodes_json_and_group_relations(stores):
    memory, _, space, console, _ = stores
    sid, uid = space["space_id"], space["logical_user_id"]
    await memory.upsert_profile(space_id=sid, logical_user_id=uid, tags=["旧"], description="old")
    generation = await memory.create_generation(sid)
    await memory.activate_generation(sid, generation["generation_id"])
    await memory.upsert_profile(space_id=sid, logical_user_id=uid, tags=["新"], description="new")
    await memory.upsert_relationship(space_id=sid, logical_user_id=uid, state_text="I trust them")
    await memory.upsert_short_term(space_id=sid, conversation_id="c", summary_text="We are discussing tests",
                                    coverage={"stream_id": "s", "to_sequence": 4})
    profiles = await console.context(sid, "profiles", q="新")
    assert profiles["total"] == 1
    assert profiles["generation_id"] == generation["generation_id"]
    assert [i["description"] for i in profiles["items"]] == ["new"]
    assert profiles["items"][0]["tags"] == ["新"]
    assert "tags_json" not in profiles["items"][0]
    assert (await console.context(sid, "profiles", q="old"))["items"] == []
    assert (await console.context(sid, "relationships"))["items"][0]["state"]["long_term"]["body"] == "I trust them"
    assert (await console.context(sid, "short_term"))["items"][0]["coverage"]["to_sequence"] == 4
    group = await memory.get_or_create_space(group_space("p", "a", "g"))
    assert (await console.context(group["space_id"], "relationships"))["items"] == []


@pytest.mark.asyncio
async def test_node_filter_is_literal_and_precedes_pagination(stores):
    memory, _, space, console, _ = stores
    await make_node(stores, "old match 50%_")
    generation = await memory.create_generation(space["space_id"])
    await memory.activate_generation(space["space_id"], generation["generation_id"])
    for i in range(4):
        await make_node(stores, f"hit {i} 50%_", status="disabled")
        await make_node(stores, f"unrelated {i}")
    first = await console.nodes(space["space_id"], q="50%_", status="disabled", node_type="event", limit=2)
    second = await console.nodes(space["space_id"], q="50%_", status="disabled", limit=2, offset=2)
    assert first["total"] == second["total"] == 4
    assert first["has_more"] and not second["has_more"]
    ids = [row["node_id"] for row in first["items"] + second["items"]]
    assert len(ids) == len(set(ids)) == 4
    assert all(row["generation_id"] == generation["generation_id"] for row in first["items"])
    assert (await console.nodes(space["space_id"], q="50__"))["items"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["nodes", "profiles", "relationships"])
async def test_memory_display_uses_current_names_outside_transaction_and_preserves_canonical(stores, monkeypatch, kind):
    memory, sources, space, console, _ = stores
    sid, uid = space["space_id"], space["logical_user_id"]
    reference = "{{user:" + uid + "}}"
    text = f"我记得{reference}让我叫她‘小鹿’，这个称呼让我感到亲切。"
    if kind == "nodes":
        await make_node(stores, text)
        await make_node(stores, f"我与{reference}一起散步。")
        field = "content_text"
    elif kind == "profiles":
        await memory.upsert_profile(space_id=sid, logical_user_id=uid, tags=[f"{reference}喜欢散步"], description=text)
        field = "description"
    else:
        saved = await memory.upsert_relationship(space_id=sid, logical_user_id=uid, state_text=text)
        field = "state_text"
        text = saved[field]
    original = sources.get_user_display_names
    lookups = []

    async def names(ids):
        assert not memory.database._lock.locked()
        lookups.append(tuple(ids))
        return await original(ids)

    monkeypatch.setattr(sources, "get_user_display_names", names)

    async def listing():
        return await console.nodes(sid) if kind == "nodes" else await console.context(sid, kind)

    first = await listing()
    assert lookups == [(uid,)]
    for item in first["items"]:
        assert reference in item[field]
        assert item["display_" + field] == item[field].replace(reference, "Alice")
    await sources.database.execute("UPDATE logical_users SET display_name=? WHERE logical_user_id=?", ("Alicia", uid))
    second = await listing()
    assert lookups == [(uid,), (uid,)]
    for item in second["items"]:
        assert item["display_" + field] == item[field].replace(reference, "Alicia")
    if kind in {"profiles", "relationships"}:
        assert first["items"][0]["logical_user_name"] == "Alice"
        assert second["items"][0]["logical_user_name"] == "Alicia"
    original_item = next(item for item in second["items"] if item[field] == text)
    assert "‘小鹿’" in original_item["display_" + field]
    assert original_item["content_version"] == 1
    if kind == "profiles":
        assert original_item["tags"] == [f"{reference}喜欢散步"]
        assert original_item["display_tags"] == ["Alicia喜欢散步"]


@pytest.mark.asyncio
async def test_display_fallback_does_not_rewrite_raw_sources_or_notes(stores):
    memory, sources, space, console, _ = stores
    reference = "{{user:unmapped-member}}"
    node = await make_node(stores, f"我与{reference}交谈，也保留‘用户’这个原文词语。")
    fallback = await ConsoleStore(memory).nodes(space["space_id"])
    item = fallback["items"][0]
    assert item["content_text"] == node["content_text"]
    assert item["display_content_text"] == "我与未命名用户交谈，也保留‘用户’这个原文词语。"
    raw = await sources.capture(make_source("literal-placeholder", reference))
    source = (await console.source(space["space_id"], raw.source_id))["source"]
    assert source["content_text"] == reference and "display_content_text" not in source
    await memory.create_note(space_id=space["space_id"], content_text=reference)
    note = (await console.notes(space["space_id"]))["items"][0]
    assert note["content_text"] == reference and "display_content_text" not in note


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["nodes", "profiles", "relationships", "profile_tags"])
async def test_nickname_search_tracks_current_name_and_keeps_literal_search(stores, monkeypatch, kind):
    memory, sources, space, console, _ = stores
    sid, uid = space["space_id"], space["logical_user_id"]
    reference = "{{user:" + uid + "}}"
    text = f"我记得{reference}练习合唱，约定称呼是队长。"
    if kind == "nodes":
        await make_node(stores, text)
    elif kind in {"profiles", "profile_tags"}:
        await memory.upsert_profile(space_id=sid, logical_user_id=uid,
            tags=[reference] if kind == "profile_tags" else ["合唱"],
            description="练习合唱，约定称呼是队长。" if kind == "profile_tags" else text)
    else:
        await memory.upsert_relationship(space_id=sid, logical_user_id=uid, state_text=text)
    original_lookup = sources.find_user_ids_by_display_name
    lookups = []

    async def lookup(query):
        assert not memory.database._lock.locked()
        lookups.append(query)
        return await original_lookup(query)

    monkeypatch.setattr(sources, "find_user_ids_by_display_name", lookup)

    async def listing(query, store=console):
        if kind == "nodes":
            return await store.nodes(sid, q=query)
        return await store.context(sid, "profiles" if kind == "profile_tags" else kind, q=query)

    assert len((await listing("aLiCe"))["items"]) == 1
    assert len((await listing(reference))["items"]) == 1
    assert len((await listing("合唱"))["items"]) == 1
    assert len((await listing("队长"))["items"]) == 1
    assert (await listing("Alice", ConsoleStore(memory)))["items"] == []
    assert len((await listing(reference, ConsoleStore(memory)))["items"]) == 1
    before = list(lookups)
    assert len((await listing(""))["items"]) == 1
    assert lookups == before
    await sources.database.execute(
        "UPDATE logical_users SET display_name=? WHERE logical_user_id=?", ("Alicia", uid))
    renamed = (await listing("ALICIA"))["items"]
    assert len(renamed) == 1
    assert renamed[0]["content_version"] == 1
    assert (await listing("Alice"))["items"] == []


@pytest.mark.asyncio
async def test_same_nickname_search_preserves_identity_and_filters_scope_and_generation(stores):
    memory, sources, space, console, _ = stores
    sid, first_uid = space["space_id"], space["logical_user_id"]
    await sources.bind_account(platform_instance_id="other-platform", account_id="other-account",
        logical_user_id="second-member", display_name="Alice")
    first_ref = "{{user:" + first_uid + "}}"
    second_ref = "{{user:second-member}}"
    retired = await make_node(stores, "我记得" + first_ref + "过去练习合唱。")
    generation = await memory.create_generation(sid)
    await memory.activate_generation(sid, generation["generation_id"])
    first = await make_node(stores, "我记得" + first_ref + "喜欢合唱。")
    second = await make_node(stores, "我记得" + second_ref + "喜欢舞蹈。")
    other = await memory.get_or_create_space(private_space("other-persona", first_uid))
    source_id = (await sources.list_records())[0]["source_id"]
    foreign = await memory.create_node(space_id=other["space_id"], node_type="event",
        content_text="我记得" + second_ref + "参加演出。", source_ids=[source_id])
    one = await console.nodes(sid, q="Alice", limit=1)
    two = await console.nodes(sid, q="Alice", limit=1, offset=1)
    items = one["items"] + two["items"]
    assert one["has_more"] and not two["has_more"]
    assert {item["node_id"] for item in items} == {first["node_id"], second["node_id"]}
    assert retired["node_id"] not in {item["node_id"] for item in items}
    assert foreign["node_id"] not in {item["node_id"] for item in items}
    assert {item["content_text"] for item in items} == {first["content_text"], second["content_text"]}
    assert all("Alice" in item["display_content_text"] for item in items)

    group = await memory.get_or_create_space(group_space("group-persona", "adapter-1", "room"))
    for uid in (first_uid, "second-member"):
        await memory.upsert_profile(space_id=group["space_id"], logical_user_id=uid,
            tags=[], description="{{user:" + uid + "}}有自己的爱好。")
    await memory.upsert_profile(space_id=other["space_id"], logical_user_id=first_uid,
        tags=[], description=first_ref + "的其他人格资料。")
    profiles = (await console.context(group["space_id"], "profiles", q="Alice"))["items"]
    choices = await console.profile_users(group["space_id"])
    assert {(it["display_name"], it["logical_user_id"]) for it in choices} == {("Alice", first_uid), ("Alice", "second-member")}
    assert [it["logical_user_id"] for it in (await console.context(group["space_id"], "profiles", logical_user_id=first_uid))["items"]] == [first_uid]
    assert {item["logical_user_id"] for item in profiles} == {first_uid, "second-member"}
    assert all(item["space_id"] == group["space_id"] for item in profiles)


@pytest.mark.asyncio
async def test_source_requires_matching_source_scope_and_survives_generation_rebuild(stores):
    memory, sources, space, console, _ = stores
    node = await make_node(stores)
    source_id = (await memory.list_node_sources(node["node_id"]))[0]["source_id"]
    result = await console.source(space["space_id"], source_id)
    assert result["source"]["content_text"]
    assert "metadata_json" not in result["source"]
    assert isinstance(result["source"]["metadata"], dict)
    other = await memory.get_or_create_space(private_space("persona-other", "another"))
    with pytest.raises(KeyError):
        await console.source(other["space_id"], source_id)
    # Even an invalid imported reference must not grant access to another source scope.
    await memory.create_node(space_id=other["space_id"], node_type="event", content_text="bad reference",
                             source_ids=[source_id])
    with pytest.raises(KeyError):
        await console.source(other["space_id"], source_id)
    foreign = await sources.capture(replace(make_source("foreign"), persona_id="foreign-persona"))
    with pytest.raises(KeyError):
        await console.source(space["space_id"], foreign.source_id)
    generation = await memory.create_generation(space["space_id"])
    await memory.activate_generation(space["space_id"], generation["generation_id"])
    assert (await console.source(space["space_id"], source_id))["source"]["source_id"] == source_id


@pytest.mark.asyncio
async def test_source_removed_is_unavailable_but_generation_swap_preserves_raw_access(stores, monkeypatch):
    memory, sources, space, console, _ = stores
    node = await make_node(stores)
    source_id = (await memory.list_node_sources(node["node_id"]))[0]["source_id"]
    original = sources.get_records

    async def swapping(ids):
        result = await original(ids)
        generation = await memory.create_generation(space["space_id"])
        await memory.activate_generation(space["space_id"], generation["generation_id"])
        return result

    monkeypatch.setattr(sources, "get_records", swapping)
    assert (await console.source(space["space_id"], source_id))["source"]["source_id"] == source_id
    monkeypatch.setattr(sources, "get_records", original)
    await make_node(stores)
    await sources.database.execute("DELETE FROM source_records WHERE source_id=?", (source_id,))
    with pytest.raises(KeyError):
        await console.source(space["space_id"], source_id)


@pytest.mark.asyncio
async def test_note_lookup_resolves_renamed_people_and_preserves_literal_text(stores):
    memory, sources, space, console, _ = stores
    sid, uid = space["space_id"], space["logical_user_id"]
    original = "和{{user:" + uid + "}}聊起另一位叫 Alice 的人。"
    await memory.create_note(space_id=sid, content_text=original)
    await sources.rename_logical_user(uid, "小林")
    page = await console.notes(sid, q="小林")
    assert page["items"][0]["content_text"] == original
    assert page["user_names"] == {uid: "小林"}


@pytest.mark.asyncio
async def test_notes_are_independent_of_generation_and_filter_before_pagination(stores):
    memory, _, space, console, service = stores
    created = await execute(service, space, "create", {"action": "create", "content_text": "Please keep 20%_"})
    generation = await memory.create_generation(space["space_id"])
    await memory.activate_generation(space["space_id"], generation["generation_id"])
    assert (await console.notes(space["space_id"]))["items"][0]["note_id"] == created["note_id"]
    archived = await execute(service, space, "archive", {"action": "archive", "note_id": created["note_id"],
                                                         "expected_updated_at": created["updated_at"]})
    assert (await console.notes(space["space_id"]))["items"] == []
    assert (await console.notes(space["space_id"], status="archived", q="20%_"))["items"][0]["status"] == "archived"
    restored = await execute(service, space, "restore", {"action": "restore", "note_id": created["note_id"],
                                                         "expected_updated_at": archived["updated_at"]})
    assert restored["status"] == "active" and restored["updated_at"] > archived["updated_at"]
    await execute(service, space, "other", {"action": "create", "content_text": "not matching"})
    filtered = await console.notes(space["space_id"], q="20%_", limit=1)
    assert len(filtered["items"]) == 1 and not filtered["has_more"]
    assert filtered["total"] == 1
    assert (await console.notes(space["space_id"], q="20%_", offset=30))["total"] == 1
    assert filtered["items"][0]["source_kind"] == "manual"


@pytest.mark.asyncio
async def test_note_stale_page_cross_space_and_direct_mutation_conflicts(stores):
    memory, _, space, _, service = stores
    created = await execute(service, space, "create", {"action": "create", "content_text": "first"})
    operation = {"action": "update", "note_id": created["note_id"], "expected_updated_at": created["updated_at"],
                 "content_text": "second"}
    await execute(service, space, "edit", operation)
    with pytest.raises(JobConflict):
        await service.enqueue(space["space_id"], "stale", operation)
    other = await memory.get_or_create_space(private_space("p", "other"))
    with pytest.raises(KeyError):
        await service.enqueue(other["space_id"], "cross", operation)
    # A stale page cannot bypass checks by changing an action to archive.
    with pytest.raises(JobConflict):
        await service.enqueue(space["space_id"], "stale-archive", {"action": "archive", "note_id": created["note_id"],
                                                                  "expected_updated_at": created["updated_at"]})


@pytest.mark.asyncio
async def test_note_duplicate_submission_and_receipt_survive_restart(stores):
    memory, _, space, console, service = stores
    operation = {"action": "create", "content_text": "one note only"}
    first, duplicate = await asyncio.gather(service.enqueue(space["space_id"], "same", operation),
                                            service.enqueue(space["space_id"], "same", operation))
    assert first["job_id"] == duplicate["job_id"]
    claimed = await service.jobs.claim("before-restart")
    result = await service.run(claimed)
    # Simulate process death after mutation/receipt commit but before runner finish.
    await memory.database.execute("UPDATE memory_jobs SET lease_until=0 WHERE job_id=?", (first["job_id"],))
    await memory.close()
    await memory.open()
    resumed = await service.jobs.claim("after-restart")
    replay = await service.run(resumed)
    assert replay["replayed"] and replay["note_id"] == result["note_id"]
    await service.jobs.finish(resumed["job_id"], resumed["lease_token"], result=replay)
    repeated = await service.enqueue(space["space_id"], "same", operation)
    assert repeated["state"] == "succeeded"
    assert len((await console.notes(space["space_id"]))["items"]) == 1
    with pytest.raises(JobConflict):
        await service.enqueue(space["space_id"], "same", {"action": "create", "content_text": "different"})


@pytest.mark.asyncio
async def test_note_duplicate_update_returns_finished_job_despite_new_revision(stores):
    _, _, space, _, service = stores
    created = await execute(service, space, "create", {"action": "create", "content_text": "first"})
    operation = {"action": "update", "note_id": created["note_id"], "expected_updated_at": created["updated_at"],
                 "content_text": "second"}
    await execute(service, space, "edit", operation)
    assert (await service.enqueue(space["space_id"], "edit", operation))["state"] == "succeeded"


@pytest.mark.asyncio
async def test_note_scope_exclusion_is_bidirectional_and_other_spaces_continue(stores):
    memory, _, space, _, service = stores
    job = await service.enqueue(space["space_id"], "create", {"action": "create", "content_text": "one"})
    with pytest.raises(JobConflict):
        await service.jobs.submit("rebuild", space["space_id"], {}, "rebuild")
    with pytest.raises(JobConflict):
        await service.jobs.submit("reindex", "*", {}, "global")
    other = await memory.get_or_create_space(private_space("p", "other"))
    await service.jobs.submit("extract", other["space_id"], {}, "other-space")
    with pytest.raises(JobConflict):
        await service.enqueue(other["space_id"], "blocked", {"action": "create", "content_text": "blocked"})
    await service.jobs.cancel(job["job_id"])
    await service.jobs.submit("rebuild", space["space_id"], {}, "now-rebuild")
    with pytest.raises(JobConflict):
        await service.enqueue(space["space_id"], "second", {"action": "create", "content_text": "blocked"})


@pytest.mark.asyncio
async def test_note_cancel_and_expired_lease_cannot_commit(stores):
    memory, _, space, console, service = stores
    await service.enqueue(space["space_id"], "create", {"action": "create", "content_text": "cancelled"})
    job = await service.jobs.claim("worker")
    await service.jobs.cancel(job["job_id"])
    with pytest.raises(LeaseLost):
        await service.run(job)
    assert (await console.notes(space["space_id"]))["items"] == []
    await service.jobs.retry(job["job_id"])
    job = await service.jobs.claim("worker")
    await memory.database.execute("UPDATE memory_jobs SET lease_until=0 WHERE job_id=?", (job["job_id"],))
    with pytest.raises(LeaseLost):
        await service.run(job)
    assert (await console.notes(space["space_id"]))["items"] == []


@pytest.mark.asyncio
async def test_note_rechecks_revision_before_commit(stores):
    memory, _, space, _, service = stores
    created = await execute(service, space, "create", {"action": "create", "content_text": "first"})
    await service.enqueue(space["space_id"], "edit", {"action": "update", "note_id": created["note_id"],
                         "expected_updated_at": created["updated_at"], "content_text": "stale"})
    # Model a legacy writer/import outside the note service; never silently overwrite it.
    await memory.database.execute("UPDATE notes SET updated_at=updated_at+1 WHERE note_id=?", (created["note_id"],))
    job = await service.jobs.claim("worker")
    with pytest.raises(ReviewRequired, match="note_revision_changed"):
        await service.run(job)


@pytest.mark.asyncio
@pytest.mark.parametrize("persona", ["default", "_chatui_default_", "[%None]"])
async def test_default_persona_note_writes_are_rejected(stores, persona):
    memory, _, _, _, service = stores
    space = await memory.get_or_create_space(private_space(persona, "u"))
    with pytest.raises(ValueError):
        await service.enqueue(space["space_id"], "unsupported", {"action": "create", "content_text": "no"})


@pytest.mark.parametrize("operation", [None, {}, {"action": []}, {"action": "delete"},
    {"action": "create", "content_text": 4}, {"action": "create", "content_text": ""},
    {"action": "create", "content_text": "yes", "note_id": "chosen"},
    {"action": "update", "note_id": "n", "content_text": "yes"},
    {"action": "archive", "note_id": "n", "expected_updated_at": True},
    {"action": "restore", "note_id": "n", "expected_updated_at": float("nan")},
])
def test_note_operation_schema_rejects_ambiguous_commands(operation):
    with pytest.raises(ValueError):
        validate_note_operation(operation)


@pytest.mark.asyncio
async def test_unprocessed_source_is_reviewable_but_pending_identity_is_not(stores):
    _, sources, space, console, _ = stores
    record = await sources.capture(make_source("not-extracted"))
    assert (await console.source(space["space_id"], record.source_id))["source"]["message_id"] == "not-extracted"
    pending = await sources.capture(replace(make_source("pending"), identity_pending=True))
    with pytest.raises(KeyError):
        await console.source(space["space_id"], pending.source_id)


@pytest.mark.asyncio
async def test_group_source_scope_checks_persona_platform_and_group(stores):
    from tests.test_source_scan import different_stream

    memory, sources, _, console, _ = stores
    space = await memory.get_or_create_space(group_space("p", "a", "g"))
    valid = await sources.capture(different_stream("good", platform="a", group="g", persona="p"))
    assert (await console.source(space["space_id"], valid.source_id))["source"]["message_id"] == "good"
    for record in (
        different_stream("persona", platform="a", group="g", persona="q"),
        different_stream("platform", platform="b", group="g", persona="p"),
        different_stream("group", platform="a", group="h", persona="p"),
        different_stream("private", platform="a", persona="p"),
    ):
        captured = await sources.capture(record)
        with pytest.raises(KeyError):
            await console.source(space["space_id"], captured.source_id)


@pytest.mark.asyncio
async def test_private_source_cannot_be_accessed_from_another_logical_user(stores):
    memory, sources, _, console, _ = stores
    space = await memory.get_or_create_space(private_space("persona-default", "another-user"))
    record = await sources.capture(make_source("private-user"))
    with pytest.raises(KeyError):
        await console.source(space["space_id"], record.source_id)


@pytest.mark.asyncio
async def test_old_default_persona_note_task_is_skipped(stores):
    memory, _, _, console, service = stores
    space = await memory.get_or_create_space(private_space("default", "u"))
    await service.jobs.submit("note", space["space_id"], {"operation": {"action": "create", "content_text": "legacy"}}, "legacy")
    claimed = await service.jobs.claim("worker")
    assert await service.run(claimed) == {"skipped": True, "reason": "unsupported_persona"}
    assert (await console.notes(space["space_id"]))["items"] == []


@pytest.mark.asyncio
async def test_note_updated_at_remains_strictly_increasing_if_clock_moves_back(stores, monkeypatch):
    from types import SimpleNamespace

    import storage.console_store as module

    _, _, space, _, service = stores
    first = await execute(service, space, "create", {"action": "create", "content_text": "original"})
    monkeypatch.setattr(module, "time", SimpleNamespace(time=lambda: first["updated_at"] - 100))
    second = await execute(service, space, "update", {"action": "update", "note_id": first["note_id"],
        "expected_updated_at": first["updated_at"], "content_text": "edited"})
    assert second["updated_at"] > first["updated_at"]
    with pytest.raises(JobConflict):
        await service.enqueue(space["space_id"], "stale", {"action": "archive", "note_id": first["note_id"],
                                                          "expected_updated_at": first["updated_at"]})


@pytest.mark.asyncio
async def test_context_read_keeps_one_snapshot_during_external_generation_switch(stores, monkeypatch):
    import storage.console_store as module

    memory, _, space, console, _ = stores
    sid, uid = space["space_id"], space["logical_user_id"]
    await memory.upsert_profile(space_id=sid, logical_user_id=uid, tags=[], description="old snapshot")
    old_generation = await memory.get_active_generation(sid)
    external = MemoryStore(memory.path)
    await external.open()
    original = module._space
    switched = False

    async def switching(connection, space_id):
        nonlocal switched
        selected = await original(connection, space_id)
        if not switched:
            switched = True
            target = await external.create_generation(sid)
            await external.activate_generation(sid, target["generation_id"])
            await external.upsert_profile(space_id=sid, logical_user_id=uid, tags=[], description="new snapshot")
        return selected

    monkeypatch.setattr(module, "_space", switching)
    try:
        result = await console.context(sid, "profiles")
        assert result["generation_id"] == old_generation["generation_id"]
        assert result["items"][0]["description"] == "old snapshot"
        assert (await console.context(sid, "profiles"))["items"][0]["description"] == "new snapshot"
    finally:
        await external.close()


@pytest.mark.asyncio
async def test_topic_members_and_parent_topics_are_paged_scoped_and_rendered(stores):
    memory, sources, space, console, _ = stores
    sid = space["space_id"]
    source_id = (await sources.list_records())[0]["source_id"]
    topics = [await memory.create_node(space_id=sid, node_type="topic", content_text="持续关联",
              title="{{user:" + space["logical_user_id"] + "}}的项目", source_ids=[source_id]) for _ in range(2)]
    events = [await make_node(stores, f"独立经历 {index}") for index in range(3)]
    for topic in topics:
        for event in events:
            await memory.add_edge(topic["node_id"], event["node_id"], "contains")
    await memory.add_edge(topics[0]["node_id"], topics[1]["node_id"], "related")
    first = await console.node_relations(sid, topics[0]["node_id"], limit=2)
    second = await console.node_relations(sid, topics[0]["node_id"], limit=2, offset=2)
    assert first["relation"] == "members" and first["has_more"] and not second["has_more"]
    assert {row["node_id"] for row in first["items"] + second["items"]} == {row["node_id"] for row in events}
    parents = await console.node_relations(sid, events[0]["node_id"])
    assert parents["relation"] == "topics"
    assert {row["node_id"] for row in parents["items"]} == {row["node_id"] for row in topics}
    assert all(row["display_title"] == "Alice的项目" for row in parents["items"])
    other = await memory.get_or_create_space(private_space("other", "u"))
    with pytest.raises(KeyError):
        await console.node_relations(other["space_id"], events[0]["node_id"])
    generation = await memory.create_generation(sid)
    await memory.activate_generation(sid, generation["generation_id"])
    with pytest.raises(KeyError):
        await console.node_relations(sid, topics[0]["node_id"])


@pytest.mark.asyncio
async def test_node_content_time_orders_pages_and_survives_source_cleanup(stores):
    memory, sources, space, console, _ = stores
    nodes = [await make_node(stores, f"content time {index}") for index in range(4)]
    times = [200, None, 100, 200]
    for index, (node, value) in enumerate(zip(nodes, times)):
        await memory.database.execute(
            "UPDATE memory_nodes SET content_updated_at=?,updated_at=? WHERE node_id=?",
            (value, 1000 + index, node["node_id"]),
        )
    expected = sorted([nodes[0]["node_id"], nodes[3]["node_id"]]) + [nodes[2]["node_id"], nodes[1]["node_id"]]
    first = await console.nodes(space["space_id"], limit=2)
    second = await console.nodes(space["space_id"], limit=2, offset=2)
    assert first["has_more"] and not second["has_more"]
    items = first["items"] + second["items"]
    assert [item["node_id"] for item in items] == expected
    assert [item["content_updated_at"] for item in items] == [200, 200, 100, None]
    assert {item["node_id"]: item["updated_at"] for item in items} == {
        node["node_id"]: 1000 + index for index, node in enumerate(nodes)
    }
    await sources.database.execute("DELETE FROM source_records")
    after = await console.nodes(space["space_id"])
    assert [(item["node_id"], item["content_updated_at"]) for item in after["items"]] == [
        (item["node_id"], item["content_updated_at"]) for item in items
    ]


@pytest.mark.asyncio
async def test_topic_relations_use_content_time_and_preserve_internal_revision(stores):
    memory, sources, space, console, _ = stores
    sid = space["space_id"]
    source_id = (await sources.list_records())[0]["source_id"]
    topics = [await memory.create_node(space_id=sid, node_type="topic", content_text=f"topic {index}",
                                      source_ids=[source_id])
              for index in range(3)]
    events = [await make_node(stores, f"event {index}") for index in range(3)]
    for collection in (topics, events):
        for index, (node, value) in enumerate(zip(collection, [100, 200, None])):
            await memory.database.execute(
                "UPDATE memory_nodes SET content_updated_at=?,updated_at=? WHERE node_id=?",
                (value, 300 - index, node["node_id"]),
            )
    for topic in topics:
        for event in events:
            await memory.add_edge(topic["node_id"], event["node_id"], "contains")
    for origin, collection in ((topics[0], events), (events[0], topics)):
        first = await console.node_relations(sid, origin["node_id"], limit=2)
        second = await console.node_relations(sid, origin["node_id"], limit=2, offset=2)
        assert first["has_more"] and not second["has_more"]
        items = first["items"] + second["items"]
        assert [item["node_id"] for item in items] == [collection[i]["node_id"] for i in (1, 0, 2)]
        assert [item["content_updated_at"] for item in items] == [200, 100, None]
        assert [item["updated_at"] for item in items] == [299, 300, 298]


@pytest.mark.asyncio
async def test_source_and_version_details_resolve_nested_identity_fields_after_rename(stores):
    _, sources, space, console, _ = stores
    uid = space["logical_user_id"]
    token = "{{user:" + uid + "}}"
    sid = space["space_id"]
    raw = await sources.capture(make_source("shared-reference", "致 " + token))
    await sources.rename_logical_user(uid, "新的名字")
    record = await console.source(sid, raw.source_id)
    assert record["source"]["actor_logical_user_id"] == uid
    assert record["source"]["content_text"] == "致 " + token
    shown = record["display_source"]
    assert shown["actor_logical_user_name"] == shown["conversation_logical_user_name"] == "新的名字"
    assert shown["content_text"] == "致 新的名字"
    assert "actor_logical_user_id" not in shown

    version = {"derivations": [{"semantic": {"speakers": [{"logical_user_id": uid}], "text": token}}],
               "state": {"views": [token]}}
    rendered, = await console.render_items([version])
    assert rendered["display_state"] == {"views": ["新的名字"]}
    assert rendered["derivations"][0]["display_semantic"] == {
        "speakers": [{"logical_user_name": "新的名字"}], "text": "新的名字"}
    assert version["state"] == {"views": [token]}
    participant, = await console.display_details([{
        "participant_kind": "logical_user", "participant_id": uid, "display_name": "旧名称"}])
    assert participant == {"participant_kind": "logical_user", "participant_name": "新的名字"}
