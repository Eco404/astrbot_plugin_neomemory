"""Whole-context identity rendering precedes byte-budget selection."""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from core.active_recall import merge_active_result
from core.relationship_state import relationship_slots
from core.context_bundle import assemble_context
from core.memory_text import render_memory_item, render_memory_text
from core.retrieval import RecallPolicy, RecallResult, render_nodes, select_context
from storage.console_store import ConsoleStore

A = 'lu_shared_prefix_000000000000000000000000a'
B = 'lu_shared_prefix_000000000000000000000000b'
NICKNAME = '同昵称🌠'
NAMES = {A: NICKNAME, B: NICKNAME}
DISPLAY_A = f'{NICKNAME}（{A}）'
DISPLAY_B = f'{NICKNAME}（{B}）'


def reference(identity):
    return '{{user:' + identity + '}}'


def node(identity=A, identifier='memory-a', **extra):
    return {'node_id': identifier, 'node_type': 'event', 'status': 'active',
        'content_version': 1, 'direct': True,
        'content_text': reference(identity) + '负责登记，尚未接手搬运。', **extra}


def settings(budget=10000, **extra):
    return SimpleNamespace(recall_budget_tokens=budget, tools_enabled=False,
        context_short_term_max_chars=10000, context_profile_max_chars=10000,
        context_relationship_max_chars=10000, **extra)


def local(**extra):
    return {'profile': None, 'relationship': None, 'short_term': None, 'active_notes': 0, **extra}


def test_same_nickname_uses_full_identity_even_in_separate_single_person_entries():
    # The identities deliberately share a long prefix: truncation loses the distinction.
    assert render_memory_text(reference(A), NAMES) == DISPLAY_A
    assert render_memory_text(reference(B), NAMES) == DISPLAY_B
    assert DISPLAY_A != DISPLAY_B
    assert render_memory_text(reference(A) + '与' + reference(B), NAMES) == DISPLAY_A + '与' + DISPLAY_B


def test_only_distinct_ids_with_equal_nonempty_names_need_disambiguation():
    names = {'a': '阿禾', 'b': '阿禾', 'c': '小星', 'd': '小星', 'e': '小海', 'f': '', 'g': ''}
    text = '、'.join(reference(key) for key in names)
    assert render_memory_text(text, names) == '阿禾（a）、阿禾（b）、小星（c）、小星（d）、小海、未命名成员（f）、未命名成员（g）'
    # Several accounts of one logical user still resolve to a single mapping key.
    assert render_memory_text(reference(A) + '又提到' + reference(A), {A: NICKNAME}) == NICKNAME + '又提到' + NICKNAME
    assert render_memory_text(reference(A), {}) == f'未命名成员（{A}）'
    assert render_memory_text(reference(A)) == f'未命名成员（{A}）'


def test_literal_special_names_and_one_pass_substitution_are_preserved():
    names = dict(NAMES)
    original = deepcopy(names)
    literal = f'约好称呼“{NICKNAME}”；字面昵称和旧称呼小南不改。'
    assert render_memory_text(literal + reference(A), names) == literal + DISPLAY_A
    assert names == original
    injected = {'a': reference('b'), 'b': reference('b')}
    assert render_memory_text(reference('a'), injected) == reference('b') + '（a）'


def test_item_title_prose_emotion_profile_relationship_and_summary_share_identity():
    item = {'node_id': 'node', 'title': reference(A) + '的安排',
        'content_text': reference(A) + '负责登记，' + reference(B) + '负责搬运。',
        'description': reference(A) + '偏好早班。', 'state_text': '我愿意听' + reference(B) + '说。',
        'event_time_text': reference(A) + '说明的星期三',
        'summary_text': reference(A) + '等' + reference(B) + '确认。',
        'tags': ['固定称呼小南', reference(A)],
        'emotion': {'tags': [{'tag': 'concern', 'intensity': .4}], 'description': '我担心' + reference(B) + '太累。'}}
    original = deepcopy(item)
    rendered = render_memory_item(item, NAMES)
    assert item == original
    assert all(rendered[key] == value for key, value in original.items())
    for field in ('title', 'content_text', 'description', 'state_text', 'event_time_text', 'summary_text'):
        assert rendered['display_' + field] == render_memory_text(item[field], NAMES)
    assert rendered['display_tags'] == ['固定称呼小南', DISPLAY_A]
    assert rendered['display_emotion']['description'] == '我担心' + DISPLAY_B + '太累。'


@pytest.mark.asyncio
async def test_console_list_and_title_versions_use_one_lookup_for_all_entries():
    looked_up = []
    async def names(ids):
        looked_up.append(tuple(ids))
        return {key: NAMES[key] for key in ids}
    items = [{'title': reference(A), 'content_text': reference(A) + '负责登记。'},
        {'title': reference(B), 'content_text': reference(B) + '负责搬运。',
         'derivations': [{'title_change': {'before': reference(A) + '的登记', 'after': reference(B) + '的搬运'}}]}]
    original = deepcopy(items)
    result = await ConsoleStore(None, SimpleNamespace(get_user_display_names=names)).render_items(items)
    assert looked_up == [(A, B)]
    assert result[0]['display_title'] == result[1]['display_title'] == NICKNAME
    change = result[1]['derivations'][0]['title_change']
    assert change['display_before'] == NICKNAME + '的登记'
    assert change['display_after'] == NICKNAME + '的搬运'
    assert items == original


def test_passive_selector_accounts_for_id_growth_before_accepting_whole_node():
    candidate = node()
    short_names = {A: NICKNAME}
    old_size = len(render_nodes([candidate], short_names).encode('utf-8'))
    full_size = len(render_nodes([candidate], NAMES).encode('utf-8'))
    assert full_size > old_size
    assert select_context([candidate], [], RecallPolicy(budget_tokens=old_size), user_names=short_names) == [candidate]
    assert select_context([candidate], [], RecallPolicy(budget_tokens=old_size), user_names=NAMES) == []
    assert select_context([candidate], [], RecallPolicy(budget_tokens=full_size), user_names=NAMES) == [candidate]


@pytest.mark.parametrize('priority', ['recall_first', 'context_first'])
def test_final_passive_shared_context_has_consistent_names_and_exact_utf8_budget(priority):
    snapshot = local(short_term={'summary_text': reference(A) + '等' + reference(B) + '确认。'},
        profile={'tags': [reference(A)], 'description': reference(A) + '偏好早班。'},
        relationship={'state': relationship_slots('我愿意听' + reference(B) + '慢慢说明。')})
    nodes = [node(A, 'a', event_time_text=reference(A) + '提到的周三',
        emotion={'tags': [{'tag': 'concern', 'intensity': .4}], 'description': '我担心' + reference(B) + '太累。'}), node(B, 'b')]
    original = deepcopy((snapshot, nodes, NAMES))
    whole = assemble_context(snapshot, nodes, settings(context_budget_priority=priority), user_names=NAMES)
    assert DISPLAY_A in whole.text and DISPLAY_B in whole.text
    assert '{{user:' not in whole.text
    exact = len(whole.text.encode('utf-8'))
    assert whole.diagnostics['injected_bytes'] == exact
    at_limit = assemble_context(snapshot, nodes, settings(exact, context_budget_priority=priority), user_names=NAMES)
    below = assemble_context(snapshot, nodes, settings(exact - 1, context_budget_priority=priority), user_names=NAMES)
    assert at_limit.text == whole.text
    assert len(below.text.encode('utf-8')) <= exact - 1 and below.text != whole.text
    for candidate in nodes:
        assert (render_memory_text(candidate['content_text'], NAMES) in below.text) == (candidate in below.hits)
    assert (snapshot, nodes, NAMES) == original


@pytest.mark.parametrize('section', ['long_term', 'short_term', 'profile', 'relationship'])
def test_final_passive_omits_whole_entry_when_only_full_identity_causes_overflow(section):
    snapshot, nodes = local(), []
    prose = reference(A) + '的记录。'
    if section == 'long_term':
        nodes = [node(content_text=prose)]
    elif section == 'profile':
        snapshot[section] = {'description': prose, 'tags': []}
    elif section == 'short_term':
        snapshot[section] = {'summary_text': prose}
    else:
        snapshot[section] = {'state': relationship_slots(prose)}
    original = assemble_context(snapshot, nodes, settings(), user_names={A: NICKNAME})
    budget = len(original.text.encode('utf-8'))
    baseline = assemble_context(snapshot, nodes, settings(budget), user_names={A: NICKNAME})
    expanded = assemble_context(snapshot, nodes, settings(budget), user_names=NAMES)
    assert baseline.text == original.text and baseline.text
    assert expanded.text == '' and expanded.hits == []
    if section == 'long_term':
        assert expanded.diagnostics['long_term_omitted'] == [{'node_id': 'memory-a', 'reason': 'budget'}]
    else:
        assert expanded.diagnostics['context_omitted'] == [section + ':budget']


def test_active_final_tool_envelope_refits_after_full_identity_rendering():
    candidate = node(emotion={'tags': [{'tag': 'concern', 'intensity': .4}],
        'description': '我担心' + reference(A) + '太累。'})
    plain = RecallResult('space', hits=[candidate], user_names={A: NICKNAME})
    merge_active_result(plain, [], limit=1, budget=10000)
    budget = len(plain.text.encode('utf-8'))
    expanded = RecallResult('space', hits=[candidate], candidates=[candidate], user_names=NAMES)
    assert merge_active_result(expanded, [], limit=1, budget=budget) == ''
    assert expanded.hits == []
    fitting = RecallResult('space', hits=[candidate], user_names=NAMES)
    merge_active_result(fitting, [], limit=1, budget=10000)
    payload = json.loads(fitting.text.split('\n', 1)[1])[0]
    assert payload['memory'].startswith(DISPLAY_A)
    assert payload['emotion']['description'] == '我担心' + DISPLAY_A + '太累。'
    exact = len(fitting.text.encode('utf-8'))
    at_limit = RecallResult('space', hits=[candidate], user_names=NAMES)
    assert merge_active_result(at_limit, [], limit=1, budget=exact) == fitting.text
    assert len(at_limit.text.encode('utf-8')) == exact
    assert candidate['content_text'].startswith(reference(A))
