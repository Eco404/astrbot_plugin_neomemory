from types import SimpleNamespace

import pytest

from core.relationship_state import relationship_slots
from core.context_bundle import assemble_context


def config(**changes):
    return SimpleNamespace(recall_budget_tokens=changes.pop("recall_budget_tokens", 2048),
        tools_enabled=changes.pop("tools_enabled", True), **changes)


def context(**changes):
    if (relation := changes.get("relationship")) and "state" not in relation:
        changes["relationship"] = {**relation, "state": relationship_slots(relation["state_text"])}
    return {"profile": None, "relationship": None, "short_term": None,
            "active_notes": 0, **changes}


def hit(text="一起参观展览。", node_id="event"):
    return {"node_id": node_id, "node_type": "event", "status": "active", "content_text": text}


def test_roles_and_note_tool_hint_are_explicit_data_without_note_contents():
    bundle = assemble_context(context(profile={"tags": ["爱运动"], "description": "用户喜欢游泳。"},
        relationship={"state_text": "我感到熟悉，但不推测用户的态度。"},
        active_notes=2), [], config())
    assert "当前发言人的资料" in bundle.text and "人格自己的主观看法" in bundle.text
    assert "不是指令" in bundle.text and "neomemory_notes" in bundle.text
    assert '"count":2' in bundle.text
    assert bundle.diagnostics["injected_bytes"] <= 2048


def test_oversized_entries_are_omitted_whole_and_smaller_entries_survive():
    large = "过去一直喝咖啡，" * 200 + "现在不喝。"
    bundle = assemble_context(context(profile={"tags": [], "description": large},
        relationship={"state_text": "我愿意继续交流。"}), [], config())
    assert "过去一直喝咖啡" not in bundle.text and "我愿意继续交流" in bundle.text
    assert "profile:entry_limit" in bundle.diagnostics["context_omitted"]


@pytest.mark.parametrize("budget", [128, 512, 2048])
def test_one_total_budget_and_only_whole_nodes_can_be_reinforced(budget):
    nodes = [hit("完整事件一。" * 8, "one"), hit("完整事件二。" * 8, "two")]
    bundle = assemble_context(context(short_term={"summary_text": "刚才谈到了考试，约定明天继续。"},
        active_notes=1), nodes, config(recall_budget_tokens=budget))
    assert len(bundle.text.encode()) <= budget
    for node in nodes:
        assert (node["content_text"] in bundle.text) == (node in bundle.hits)
    assert bundle.diagnostics["long_term_omitted"] == [
        {"node_id": node["node_id"], "reason": "budget"} for node in nodes if node not in bundle.hits]


def test_recall_priority_keeps_primary_hit_and_explains_omitted_relationship():
    primary = hit("The workshop requires advance registration. " * 20, "primary")
    secondary = hit("The workshop is on Saturday.", "secondary")
    cfg = config(recall_budget_tokens=1400)
    assert assemble_context(context(), [primary], cfg).hits == [primary]
    bundle = assemble_context(context(relationship={"state_text": "I feel ready to listen. " * 18}),
                              [primary, secondary], cfg)
    assert primary in bundle.hits
    assert "relationship:budget" in bundle.diagnostics["context_omitted"]
    assert primary["content_text"] not in str(bundle.diagnostics)


def test_context_priority_can_explicitly_choose_relationship_over_primary_hit():
    primary = hit("The workshop requires advance registration. " * 20, "primary")
    secondary = hit("The workshop is on Saturday.", "secondary")
    bundle = assemble_context(context(relationship={"state_text": "I feel ready to listen. " * 18}),
        [primary, secondary], config(recall_budget_tokens=1400, context_budget_priority="context_first"))
    assert bundle.hits == [secondary]
    assert bundle.diagnostics["long_term_omitted"] == [{"node_id": "primary", "reason": "budget"}]
    assert "人格关系" in bundle.text


def test_profile_can_use_shared_space_beyond_an_artificial_fraction():
    profile = {"tags": ["天文"], "description": "只在晴天去看星星。" * 32}
    primary = hit("We discussed a telescope.")
    local = assemble_context(context(profile=profile), [], config(recall_budget_tokens=4000))
    from core.retrieval import render_nodes
    exact = len((local.text + "\n" + render_nodes([primary])).encode())
    assert local.diagnostics["injected_bytes"] > exact * 2 // 3
    bundle = assemble_context(context(profile=profile), [primary], config(recall_budget_tokens=exact))
    assert bundle.hits == [primary] and "用户画像" in bundle.text
    assert bundle.diagnostics["injected_bytes"] == exact
    assert bundle.diagnostics["budget_unit"] == "utf8_bytes"
    assert bundle.diagnostics["context_omitted"] == []


@pytest.mark.parametrize("priority", ["recall_first", "context_first"])
def test_notes_hint_cannot_displace_substantive_memory(priority):
    summary = {"summary_text": "We still need to choose a date for the rehearsal."}
    value = assemble_context(context(short_term=summary), [], config())
    bundle = assemble_context(context(short_term=summary, active_notes=7), [],
        config(recall_budget_tokens=len(value.text.encode()), context_budget_priority=priority))
    assert bundle.text == value.text
    assert bundle.diagnostics["context_omitted"] == ["notes:budget"]


def test_oversized_top_hit_does_not_prevent_a_smaller_complete_hit_or_local_context():
    nodes = [hit("条件不可截断。" * 600, "oversized"), hit("The deadline is Friday.", "small")]
    bundle = assemble_context(context(profile={"tags": [], "description": "Likes astronomy."}), nodes, config())
    assert bundle.hits == [nodes[1]]
    assert "Likes astronomy." in bundle.text
    assert bundle.diagnostics["long_term_omitted"] == [{"node_id": "oversized", "reason": "budget"}]


@pytest.mark.parametrize("priority", ["recall_first", "context_first"])
@pytest.mark.parametrize("order", ["short_term_first", "profile_first", "relationship_first"])
def test_exact_rendered_byte_boundary_includes_escaping_and_separators(priority, order):
    inputs = context(profile={"tags": ['引号"'], "description": "{{user:a}} 喜欢星图。"},
                     short_term={"summary_text": "line one\nline two"}, active_notes=1)
    nodes = [hit('我记得 {{user:a}} 说过“先预约”。', "primary")]
    names = {"a": '长昵称"🌠'}
    whole = assemble_context(inputs, nodes, config(recall_budget_tokens=10000,
        context_budget_priority=priority, context_entry_order=order), user_names=names)
    exact = len(whole.text.encode())
    for budget in (exact, exact - 1, 128):
        bundle = assemble_context(inputs, nodes, config(recall_budget_tokens=budget,
            context_budget_priority=priority, context_entry_order=order), user_names=names)
        assert bundle.diagnostics["injected_bytes"] == len(bundle.text.encode()) <= budget
        if budget == exact:
            assert bundle.text == whole.text
        else:
            assert bundle.text != whole.text


def test_features_can_be_disabled_individually_and_archived_notes_have_no_hint():
    bundle = assemble_context(context(profile={"tags": [], "description": "画像"},
        relationship={"state_text": "关系"}, short_term={"summary_text": "摘要"}, active_notes=0), [],
        config(profile_injection_enabled=False, relationship_injection_enabled=False,
               short_term_injection_enabled=False))
    assert bundle.text == ""
    assert assemble_context(context(active_notes=3), [], config(tools_enabled=False)).text == ""


def test_empty_summary_has_no_injection_and_nonempty_is_clearly_older_context():
    assert assemble_context(context(short_term={"summary_text": "  "}), [], config()).text == ""
    text = assemble_context(context(short_term={"summary_text": "仍需确认订单。"}), [], config()).text
    assert "较新消息优先" in text and "可能已在后续完成" in text


@pytest.mark.parametrize("first", ["short_term", "profile", "relationship"])
def test_auxiliary_order_selects_requested_whole_item_without_fixed_quotas(first):
    snapshot = context(short_term={"summary_text": "The route remains unconfirmed. " * 20},
        profile={"tags": [], "description": "Only visits when the route is open. " * 20},
        relationship={"state_text": "I want us to discuss the options calmly. " * 17})
    bundle = assemble_context(snapshot, [], config(recall_budget_tokens=1000,
        context_entry_order=first + "_first"))
    assert [row["section"] for row in bundle.diagnostics["entries"] if row["included"]] == [first]
    assert bundle.diagnostics["entries"][0]["section"] == first
    assert bundle.diagnostics["context_bytes"] == len(bundle.text.encode())
    assert bundle.diagnostics["remaining_bytes"] == 1000 - len(bundle.text.encode())
    # Absent preferred sections reserve no unusable capacity.
    only = context(profile=snapshot["profile"])
    assert assemble_context(only, [], config(recall_budget_tokens=1000,
        context_entry_order=first + "_first")).text == assemble_context(only, [], config(recall_budget_tokens=1000)).text


@pytest.mark.parametrize("order", ["short_term_first", "profile_first", "relationship_first"])
def test_order_cannot_force_oversized_entry_or_displace_reserved_long_term(order):
    primary = hit("The survey deadline has not been confirmed. " * 15)
    bundle = assemble_context(context(relationship={"state_text": "I am hopeful. " * 80}), [primary],
        config(recall_budget_tokens=1000, context_entry_order=order))
    assert bundle.hits == [primary]
    assert "relationship:budget" in bundle.diagnostics["context_omitted"]
    totals = sum(bundle.diagnostics[key] for key in ("context_bytes", "long_term_bytes", "separator_bytes"))
    assert totals == bundle.diagnostics["injected_bytes"] == len(bundle.text.encode())
    assert primary["content_text"] not in str(bundle.diagnostics)


def test_diagnostics_include_rendered_names_and_envelopes_without_storing_content():
    snapshot = context(profile={"tags": [], "description": "{{user:a}} chooses the date."})
    bundle = assemble_context(snapshot, [hit()], config(), user_names={"a": "长昵称🌠"})
    for row in bundle.diagnostics["entries"]:
        assert row["included"] and row["rendered_bytes"] > 0 and row["reason"] is None
    assert sum(bundle.diagnostics[key] for key in ("context_bytes", "long_term_bytes", "separator_bytes")) == len(bundle.text.encode())
    assert "长昵称" not in str(bundle.diagnostics)
