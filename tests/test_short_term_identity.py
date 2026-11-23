"""短期称呼使用来源身份，渲染跟随改名，不重写摘要或覆盖范围。"""

import json

import pytest

from core.context_bundle import assemble_context
from core.memory_text import render_memory_item
from core.forwarded_content import window_text
from core.short_term import _digest
from core.short_term_prompts import PROMPT_VERSION, SCHEMA_VERSION, messages, parse_output
from storage.console_store import ConsoleStore
from tests.test_context_bundle import config, context
from tests.test_short_term import claim, current, env, execute  # noqa: F401


def test_group_prompt_and_parser_keep_distinct_users_and_inherited_identity():
    records = [{"source_id": "s1", "actor_kind": "user", "actor_logical_user_id": "u1",
                "actor_account_id": "a", "content_text": "我带地图。", "observed_at": 10},
               {"source_id": "s2", "actor_kind": "user", "actor_logical_user_id": "u2",
                "actor_account_id": "b", "content_text": "我带水。", "observed_at": 11}]
    prior = "{{user:u3}}还没有确定是否参加。"
    prompt = messages(persona="同行伙伴", previous_summary=prior, records=records, summary_chars=400)
    assert json.loads(prompt[1]["content"])["known_user_ids"] == ["u1", "u2", "u3"]
    text = "{{user:u1}}带地图，{{user:u2}}带水；{{user:u3}}还没确定是否参加。"
    raw = json.dumps({"schema_version": SCHEMA_VERSION, "summary_text": text, "evidence_source_ids": ["s1", "s2"]})
    parsed = parse_output(raw, source_ids=["s1", "s2"], summary_chars=400,
                          previous_summary=prior, allowed_user_ids=["u1", "u2", "u3"])
    assert parsed["summary_text"] == text
    assert render_memory_item(parsed, {"u1": "秋澄", "u2": "青禾", "u3": "河舟"})["display_summary_text"] == (
        "秋澄带地图，青禾带水；河舟还没确定是否参加。")


@pytest.mark.parametrize("text", ["{{user:outside}}带水。", "{{user:u1}带水。"])
def test_unknown_or_broken_identity_is_rejected(text):
    raw = json.dumps({"schema_version": SCHEMA_VERSION, "summary_text": text, "evidence_source_ids": ["s1"]})
    with pytest.raises(ValueError, match="user reference"):
        parse_output(raw, source_ids=["s1"], summary_chars=400, allowed_user_ids=["u1"])


@pytest.mark.asyncio
async def test_summary_commit_console_search_and_injection_render_latest_nickname(env):
    user_id = env.space["logical_user_id"]
    text = "{{user:" + user_id + "}}需要准备地图；我负责查路线，小纸船是约定称呼。"
    task = await claim(env)
    env.adapter.output = json.dumps({"schema_version": SCHEMA_VERSION, "summary_text": text.replace(user_id, "u1"),
        "evidence_source_ids": [task["payload"]["source_ids"][0]]})
    assert (await execute(env, task))["state"] == "succeeded"
    stored = await current(env)
    names = {user_id: "新的昵称"}
    rendered = render_memory_item(stored, names)
    assert "新的昵称需要准备地图" in rendered["display_summary_text"]
    assert "小纸船" in rendered["display_summary_text"]
    bundle = assemble_context(context(short_term=stored), [], config(), user_names=names)
    assert "新的昵称需要准备地图" in bundle.text and "{{user:" not in bundle.text
    assert (await current(env))["summary_text"] == text
    assert (await current(env))["coverage"] == stored["coverage"]
    console = ConsoleStore(env.memory, env.sources)
    listing = await console.context(env.space["space_id"], "short_term")
    assert listing["items"][0]["display_summary_text"] != text


@pytest.mark.asyncio
async def test_unknown_model_identity_does_not_commit_summary(env):
    task = await claim(env)
    env.adapter.output = json.dumps({"schema_version": SCHEMA_VERSION,
        "summary_text": "{{user:someone_else}}已经确认。", "evidence_source_ids": [task["payload"]["source_ids"][0]]})
    result = await execute(env, task)
    assert result["error_code"] == "invalid_short_term_output"
    assert await current(env) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["short-term.v1", PROMPT_VERSION])
async def test_prepared_summary_resumes_with_original_user_aliases(env, version):
    job = await claim(env)
    user = env.space["logical_user_id"]
    payload = job["payload"]
    records = await env.sources.get_records(tuple(payload["source_ids"]))
    _, signature = await env.service._execution(env.space, version)
    raw = json.dumps({"schema_version": SCHEMA_VERSION, "summary_text": "{{user:u1}}待确认。",
                      "evidence_source_ids": [payload["source_ids"][0]]})
    checkpoint = {"signature": signature, "prompt_version": version, "user_ids": [user], "raw": raw,
                  "input_digest": _digest([payload, "", [(row["source_id"], window_text(row)) for row in records],
                                           signature])}
    await env.service._save(job, checkpoint, "short_term_validate")
    result = await execute(env, await env.jobs.get(job["job_id"]))
    assert result["state"] == "succeeded", result
    assert (await current(env))["summary_text"] == json.loads(raw)["summary_text"].replace("u1", user)
    assert not env.adapter.calls


@pytest.mark.asyncio
async def test_summary_correction_counts_window_aliases_and_commits_stable_ids(env):
    job = await claim(env)
    identity = env.space["logical_user_id"]
    env.config.short_term_summary_chars = 60
    accepted = "{{user:u1}}负责地图，{{user:u1}}明天确认。".ljust(59, "字") + "🌳"
    long_text = accepted + "。"
    async def respond():
        prompt = json.loads(env.adapter.calls[-1][1]["content"])
        assert prompt["known_user_ids"] == ["u1"] and identity not in str(prompt)
        text = long_text
        if len(env.adapter.calls) == 2:
            error = json.loads(env.adapter.calls[-1][-1]["content"])["errors"][0]
            assert error["actual_encoded_chars"] == 61 and error["constraints"]["maxLength"] == 60
            text = accepted
        env.adapter.output = json.dumps({"schema_version": SCHEMA_VERSION, "summary_text": text,
                                         "evidence_source_ids": [job["payload"]["source_ids"][0]]})

    env.adapter.hook = respond
    result = await execute(env, job)
    assert result["state"] == "succeeded", result
    assert len(env.adapter.calls) == 2
    assert (await current(env))["summary_text"] == accepted.replace("{{user:u1}}", "{{user:" + identity + "}}")


@pytest.mark.asyncio
async def test_reprepare_old_length_failure_keeps_window_and_applies_current_budget(env):
    env.config.short_term_summary_chars = 60
    job = await claim(env)
    user = env.space["logical_user_id"]
    previous = job["payload"]
    records = await env.sources.get_records(tuple(previous["source_ids"]))
    _, signature = await env.service._execution(env.space, PROMPT_VERSION)
    draft = {"schema_version": SCHEMA_VERSION,
             "summary_text": "{{user:u1}}准备路线。".ljust(61, "字"),
             "evidence_source_ids": [previous["source_ids"][0]]}
    checkpoint = {"signature": signature, "prompt_version": PROMPT_VERSION, "user_ids": [user],
                  "raw": json.dumps(draft), "corrections": 3, "correction_limit": 3,
                  "input_digest": _digest([previous, "", [(r["source_id"], window_text(r)) for r in records],
                                           signature])}
    await env.service._save(job, checkpoint, "short_term_validate")
    failed = await execute(env, await env.jobs.get(job["job_id"]))
    assert failed["error_code"] == "invalid_short_term_output"
    errors = failed["checkpoint"]["errors"]
    assert errors[0]["actual_encoded_chars"] == len(draft["summary_text"])
    assert not env.adapter.calls

    new_job = await env.service.retry(job["job_id"], fresh=True)
    assert new_job["payload"]["source_ids"] == previous["source_ids"]
    draft["summary_text"] = "{{user:u1}}准备路线。"
    env.adapter.output = json.dumps(draft)
    result = await execute(env, await env.jobs.claim("reprepared"))
    assert result["state"] == "succeeded", result
    assert (await current(env))["summary_text"] == draft["summary_text"].replace("u1", user)
    assert len(env.adapter.calls) == 1


@pytest.mark.parametrize("message_type,version,projected", [
    ("GroupMessage", PROMPT_VERSION, True),
    ("FriendMessage", PROMPT_VERSION, False),
])
def test_summary_group_addresses_use_the_same_aliases_as_speakers(message_type, version, projected):
    record = {"message_type": message_type, "stream_id": "group-window",
              "actor_kind": "user", "observed_at": 10}
    text = "请<@acct-second>把材料交给值班员。"
    sources = [
        {**record, "source_id": "s1", "message_id": "m1", "actor_logical_user_id": "member-two",
         "actor_account_id": "acct-second", "content_text": "我在二楼。"},
        {**record, "source_id": "s2", "actor_logical_user_id": "member-one",
         "actor_account_id": "acct-first", "content_text": text,
         "metadata_json": json.dumps({"group_addressing": [{"kind": "reply", "message_id": "m1"}]})}]
    prompt = messages(persona="", previous_summary="", records=sources, summary_chars=400, prompt_version=version)
    first, second = json.loads(prompt[1]["content"])["new_sources"]
    assert first["logical_user_id"] == "u2" and second["logical_user_id"] == "u1"
    if projected:
        reply, mention = second["group_addressing"]
        assert reply["logical_user_id"] == mention["logical_user_id"] == first["logical_user_id"]
        assert mention["marker"] == "{{user:u2}}"
        assert second["text"] == text.replace("<@acct-second>", mention["marker"])
        assert "member-" not in prompt[1]["content"]
    else:
        assert "group_addressing" not in second and second["text"] == text
    assert sources[1]["content_text"] == text


def test_short_term_keeps_forward_authorship_outside_group_mentions():
    rows = [
        {"source_id": "s1", "message_type": "GroupMessage", "actor_kind": "user", "actor_logical_user_id": "b",
         "actor_account_id": "b", "observed_at": 1, "content_text": "在场。"},
        {"source_id": "s2", "message_type": "GroupMessage", "actor_kind": "user", "actor_logical_user_id": "a",
         "actor_account_id": "a", "observed_at": 2, "content_text": "附言<@b>",
         "content_json": json.dumps([
             {"type": "Plain", "text": "附言"}, {"type": "At", "qq": "b"},
             {"type": "Node", "name": "清遥", "content": [{"type": "Plain", "text": "我周末去<@外部>那里。"}]},
             {"type": "Forward", "id": "card-without-content"}])}]
    request = messages(persona="", previous_summary="", records=rows, summary_chars=400)
    row = json.loads(request[1]["content"])["new_sources"][1]
    assert row["group_addressing"] == [{"kind": "mention", "actor_kind": "user", "logical_user_id": "u2",
                                        "marker": "{{user:u2}}"}]
    assert "text" not in row and row["sender_comment"] == "附言{{user:u2}}"
    assert row["forwarded_messages"] == [{"author": "清遥", "text": "我周末去<@外部>那里。"},
                                         {"content_available": False}]
    assert "转发消息" in request[0]["content"]


@pytest.mark.asyncio
async def test_forward_text_budget_limits_summary_range_and_oversized_first_source(env):
    first, second = env.records[:2]
    content_json = json.dumps([{"type": "Node", "name": "顾然", "content": "我将交付说明书。" * 100}])
    await env.sources.database.execute(
        "UPDATE source_records SET content_json=? WHERE source_id=?", (content_json, second.source_id))
    env.config.short_term_max_chars = 250
    job = await claim(env)
    assert job["payload"]["source_ids"] == [first.source_id]
    assert (await execute(env, job))["state"] == "succeeded"
    job = await claim(env)
    assert job["payload"]["source_ids"] == [second.source_id]
    assert (await execute(env, job))["error_code"] == "source_budget_exceeded"
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
async def test_prepared_summary_rechecks_forwarded_content_budget(env):
    version = PROMPT_VERSION
    job = await claim(env)
    payload = job["payload"]
    source_id = payload["source_ids"][0]
    await env.sources.database.execute(
        "UPDATE source_records SET content_json=? WHERE source_id=?",
        (json.dumps([{"type": "Node", "content": "原话" * 2000}]), source_id))
    records = await env.sources.get_records(tuple(payload["source_ids"]))
    _, signature = await env.service._execution(env.space, version)
    checkpoint = {"prompt_version": version, "signature": signature, "user_ids": [env.space["logical_user_id"]],
                  "raw": json.dumps({"schema_version": SCHEMA_VERSION, "summary_text": "{{user:u1}}正在准备面试。",
                                     "evidence_source_ids": [source_id]}),
                  "input_digest": _digest([payload, "", [(r["source_id"], window_text(r)) for r in records],
                                           signature])}
    await env.service._save(job, checkpoint, "short_term_validate")
    result = await execute(env, await env.jobs.get(job["job_id"]))
    assert result["error_code"] == "source_budget_exceeded", result
    assert not env.adapter.calls
