"""Correction obeys the prepared schema even when the draft names another one."""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from core.extraction_regeneration import validation_feedback
from core.extraction_references import ExtractionReferences, ReferenceValidationError
from core.extraction_schema import MAX_OUTPUT_CHARS, parse_extraction_output_for_context
from tests.test_regeneration_contract import complete_candidate

candidate_for = complete_candidate
from tests.test_maintenance_engine import environment as environment


def archive(candidate, *, path='$', code='invalid_output'):
    return {'error_code': code, 'window': {'raw': json.dumps(candidate, ensure_ascii=False),
        'validation_errors': [{'path': path, 'type': code}]}}


@pytest.mark.parametrize('model_version', ['extraction.v99', None, 7, 'missing'])
def test_prepared_schema_supplies_version_for_complete_business_candidate(model_version):
    schema = 'extraction.v1'
    expected = candidate_for(schema)
    candidate = deepcopy(expected)
    if model_version == 'missing':
        del candidate['schema_version']
    else:
        candidate['schema_version'] = model_version
    parsed = parse_extraction_output_for_context(json.dumps(candidate), {}, schema_version=schema,
                                                 validate_supplied_versions=False)
    assert parsed.model_dump(mode='json') == expected


def test_wrong_version_does_not_hide_independent_field_constraint():
    schema = 'extraction.v1'
    candidate = candidate_for(schema)
    candidate['schema_version'] = 'extraction.v99'
    candidate['node_changes'][0]['importance'] = 2
    errors = validation_feedback(archive(candidate), schema_version=schema)['errors']
    error, = errors
    assert error['path'] == '$.node_changes[0].importance'
    assert error['constraints']['maximum'] == 1


def test_emotion_only_update_reports_precise_tag_constraint():
    candidate = complete_candidate()
    change = candidate['node_changes'][0]
    change.update(operation='update', node_id='n1')
    change['emotion']['tags'][0]['intensity'] = 2
    context = {'known_nodes': [{'node_id': 'n1', 'content_text': change['content'], 'emotion': None}]}
    issue, = validation_feedback(archive(candidate), context=context)['errors']
    assert issue['path'] == '$.node_changes[0].emotion.tags[0].intensity'
    assert issue['constraints']['maximum'] == 1


@pytest.mark.parametrize('raw,kind', [('{"schema_version":', 'json_invalid'),
    ('{"schema_version":"x","schema_version":"y"}', 'duplicate_json_key'),
    ('{"schema_version":NaN}', 'json_invalid'), (' ' * (MAX_OUTPUT_CHARS + 1), 'output_capacity_exceeded')])
def test_target_schema_does_not_override_json_or_capacity_failure(raw, kind):
    result = validation_feedback({'error_code': 'invalid_output', 'window': {'raw': raw}}, schema_version='extraction.v1')
    assert result['errors'][0]['type'] == kind
    if raw == '{"schema_version":':
        assert result['errors'][0]['pos'] == len(raw)


@pytest.mark.parametrize('field,path', [
    ('basis', '$.node_changes[0].base_node_ids[0]'),
    ('node', '$.node_changes[0].node_id'),
    ('source', '$.node_changes[0].source_ids[0]'),
    ('user', '$.profile_changes[0].logical_user_id'),
])
def test_declared_local_node_cannot_leak_into_other_reference_domains(field, path):
    candidate = complete_candidate('extraction.v1')
    local = candidate['node_changes'][1]['local_ref']
    node = candidate['node_changes'][0]
    if field == 'basis':
        node['base_node_ids'] = [local]
    elif field == 'node':
        node.update(operation='update', node_id=local, expected_version=1)
    elif field == 'source':
        node['source_ids'] = [local]
    else:
        candidate['profile_changes'][0]['logical_user_id'] = local
    refs = ExtractionReferences(SimpleNamespace(sources=[{'source_id': 'source'}],
        known_nodes=[{'node_id': 'node'}], space={'logical_user_id': 'user'}, profiles=[], relationship=None))
    with pytest.raises(ReferenceValidationError) as caught:
        refs.decode(json.dumps(candidate))
    assert caught.value.path == path
    error = validation_feedback(archive(candidate, path=path, code='unknown_reference'), schema_version='extraction.v1')['errors'][0]
    assert error['path'] == path
    assert '或本次候选实际声明' not in error['instruction']
    assert ('不能使用' in error['instruction']) and 'local_*' in error['instruction']


@pytest.mark.parametrize('field', ['tags', 'description'])
def test_group_feedback_reports_applicable_unit_and_canonical_actual(field):
    schema = 'extraction.v1'
    candidate = candidate_for(schema)
    profile = candidate['profile_changes'][0]
    if field == 'tags':
        profile[field] = [str(i) for i in range(9)]
    else:
        profile[field] = '{{user:u1}}' + '字' * 240
    saved = archive(candidate, path=f'$.profile_changes[0].{field}', code='group_profile_limit')
    canonical = deepcopy(candidate)
    canonical['profile_changes'][0]['description'] = profile['description'].replace('u1', 'synthetic_stable_user')
    saved['window']['canonical_raw'] = json.dumps(canonical)
    error = validation_feedback(saved, schema_version=schema)['errors'][0]
    assert error['path'] == f'$.profile_changes[0].{field}'
    if field == 'tags':
        assert error['constraints'] == {'maxItems': 8} and error['actual_items'] == 9
    else:
        assert error['constraints'] == {'x-readable-maxLength': 240} and error['actual_readable_chars'] == 241


@pytest.mark.asyncio
async def test_automatic_correction_uses_prepared_target_and_commits_once(environment):
    schema, prompt = 'extraction.v1', 'extraction-prompt.v1'
    from core.extraction_apply import ExtractionService
    from tests.test_automatic_regeneration import enable
    from tests.test_maintenance_engine import enqueue_and_claim, execute

    env = environment
    enable(env)
    env.engine.extraction = ExtractionService(env.sources, env.memory, prompt_version=prompt)
    good = candidate_for(schema)
    # A node alone avoids making unrelated profile/relationship claims in this window.
    good.update(profile_changes=[], relationship_change=None, edges=[], coverage_changes=[])
    good['node_changes'] = good['node_changes'][:1]
    bad = deepcopy(good)
    bad['schema_version'] = 'extraction.v99'
    bad['node_changes'][0]['importance'] = 2
    env.adapter.output = json.dumps(bad)

    async def respond(messages):
        if len(env.adapter.calls) == 2:
            assert messages[:2] == env.adapter.calls[0]
            assert f'schema_version={schema}\n' in messages[0]['content']
            error, = json.loads(messages[-1]['content'])['errors']
            assert error['path'] == '$.node_changes[0].importance'
            assert error['constraints']['maximum'] == 1
            env.adapter.output = json.dumps(good)
    env.adapter.on_generate = respond
    completed = await execute(env, await enqueue_and_claim(env))
    assert completed['state'] == 'succeeded'
    assert len(env.adapter.calls) == 2
    assert (await env.memory.get_active_generation(env.space['space_id']))['activity_round'] == 1
    assert len(await env.memory.list_nodes(space_id=env.space['space_id'])) == 1



def test_group_profile_prompt_uses_current_storage_and_feedback_limits():
    from core.profile_memory_prompt import GROUP_PROFILE_MAX_TAGS, GROUP_PROFILE_MAX_DESCRIPTION_CHARS
    from core.prompts import build_extraction_prompt

    current = build_extraction_prompt(persona="合成人格", sources=[])[0]["content"]
    assert f"群画像描述最多{GROUP_PROFILE_MAX_DESCRIPTION_CHARS}、tags最多{GROUP_PROFILE_MAX_TAGS}项" in current


def test_production_recovery_cannot_infer_target_from_a_valid_draft():
    from core.extraction_regeneration import VERSION, RegenerationContextError, build_messages

    saved = archive(candidate_for('extraction.v1'))
    saved['candidate_id'] = 'synthetic_candidate'
    prepared = dict(input_digest='fixed_input', signature='fixed_signature', source_ids=['s1'],
                    source_cursor=1, known_node_ids=[], reference_protocol='reference.v1')
    saved['window'].update(prepared)
    window = {**prepared, 'regeneration': {'version': VERSION, 'candidate_id': saved['candidate_id']}}
    before = deepcopy((window, saved))
    with pytest.raises(RegenerationContextError) as error:
        build_messages([], window, saved)
    assert error.value.code == 'regeneration_schema_unavailable'
    assert (window, saved) == before
