"""Fixed candidate standards expose bounds without changing recovery eligibility."""
import ast
from copy import deepcopy
import inspect
import json

import pytest

from core import candidate_feedback as feedback
from core import extraction_regeneration as regeneration
from core import extraction_schema, short_term_prompts, topic_schema, topic_regeneration
from core.profile_memory_prompt import PROFILE_EDITS_PROMPT_VERSION
from core.short_term import RECOVERY_EXPLANATIONS
from storage import extraction_store, topic_store
from tests.test_regeneration_contract import complete_candidate, field_cases
from tests.test_regeneration_target_schema import archive
from tests.test_pipeline_recovery_standards import TOPIC, SUMMARY

def model_extraction_candidate():
    value = complete_candidate()
    for change in value['node_changes']:
        change.pop('expected_version', None)
    for change in value['profile_changes']:
        change.pop('expected_version', None)
        change.update(remove_tags=[], remove_sentences=[])
    if value['relationship_change']:
        value['relationship_change'].pop('expected_version', None)
    for change in value['coverage_changes']:
        change.pop('expected_event_version', None)
        change.pop('expected_atomic_version', None)
    return value


SCHEMAS = {'extraction': extraction_schema.extraction_model_json_schema(), 'topic': topic_schema.output_schema(),
           'summary': short_term_prompts.output_schema(source_ids=['s1', 's2'], summary_chars=400)}
BASES = {'extraction': model_extraction_candidate(), 'topic': TOPIC, 'summary': SUMMARY}


def errors_for(pipeline, candidate):
    raw = json.dumps(candidate)
    if pipeline == 'extraction':
        draft = archive(candidate)
        draft['window']['prompt_version'] = PROFILE_EDITS_PROMPT_VERSION
        return regeneration.validation_feedback(draft)['errors']
    with pytest.raises(ValueError) as caught:
        if pipeline == 'topic':
            topic_schema.parse_output(raw)
        else:
            short_term_prompts.parse_output(raw, source_ids=['s1', 's2'], summary_chars=400)
    return feedback.validation_errors(caught.value, raw, SCHEMAS[pipeline],
        topic_regeneration.EXPLANATIONS if pipeline == 'topic' else RECOVERY_EXPLANATIONS)


def objects(value, path=()):
    if isinstance(value, dict):
        yield path, value
        for key, item in value.items():
            yield from objects(item, (*path, key))
    elif isinstance(value, list) and value:
        yield from objects(value[0], (*path, 0))


def at(value, path):
    for key in path:
        value = value[key]
    return value


def declared_field(pipeline, path):
    schema = SCHEMAS[pipeline]

    def dereference(field):
        while '$ref' in field:
            field = schema['$defs'][field['$ref'].rsplit('/', 1)[1]]
        return field

    def nonnull(field):
        field = dereference(field)
        if 'anyOf' in field:
            field = next(option for option in field['anyOf'] if option.get('type') != 'null')
        return dereference(field)

    field = schema
    for key in path:
        field = nonnull(field)
        field = field['items'] if isinstance(key, int) else field['properties'][key]
    declared = dereference(field)
    nullable = any(option.get('type') == 'null' for option in declared.get('anyOf', []))
    return nonnull(declared), nullable


OBJECTS = [(pipeline, path, value) for pipeline, candidate in BASES.items()
           for path, value in objects(candidate)]
REQUIRED = [(pipeline, path, key) for pipeline, path, value in OBJECTS
            for key in declared_field(pipeline, path)[0].get('required', [])]


@pytest.mark.parametrize('pipeline,path,key', REQUIRED)
def test_every_required_field_names_its_nullability_and_public_type(pipeline, path, key):
    candidate = deepcopy(BASES[pipeline])
    del at(candidate, path)[key]
    if pipeline == 'extraction' and key in {'remove_tags', 'remove_sentences'}:
        # A strict provider requires the arrays. A text response can omit an edit mask to keep every old item.
        from core.profile_edits import apply_profile_edits
        profiles = candidate['profile_changes']
        apply_profile_edits(profiles, [{'logical_user_id': 'u1', 'description': '旧资料。', 'tags': ['旧标签']}])
        assert profiles[0]['description'].startswith('旧资料。') and '旧标签' in profiles[0]['tags']
        return
    errors = errors_for(pipeline, candidate)
    field_path = '$' + ''.join(f'[{part}]' if isinstance(part, int) else '.' + part for part in (*path, key))
    issue = next(error for error in errors if error['type'] == 'missing' and error['path'] == field_path)
    declared, nullable = declared_field(pipeline, (*path, key))
    constraints = issue['constraints']
    assert constraints['nullable'] is nullable
    for name in ('type', 'enum', 'const', 'pattern', 'minItems', 'maxItems', 'minLength', 'maxLength',
                 'minimum', 'maximum', 'required', 'additionalProperties', 'uniqueItems'):
        if name in declared:
            assert constraints[name] == declared[name]
    if 'x-readable-limitsByNodeType' in declared:
        assert constraints['x-readable-maxLength'] == declared['x-readable-limitsByNodeType'][at(candidate, path)['node_type']]
    elif 'x-readable-maxLength' in declared:
        assert constraints['x-readable-maxLength'] == declared['x-readable-maxLength']
    if declared.get('additionalProperties') is False:
        assert set(constraints['allowed_fields']) == set(declared['properties'])



@pytest.mark.parametrize('pipeline,path,value', OBJECTS)
def test_each_object_extra_field_reports_allowed_fields(pipeline, path, value):
    candidate = deepcopy(BASES[pipeline])
    at(candidate, path)['invented_field'] = 'unwanted'
    issues = errors_for(pipeline, candidate)
    issue = next(error for error in issues if error['type'] == 'extra_forbidden')
    assert issue['constraints']['additionalProperties'] is False
    declared, _ = declared_field(pipeline, path)
    assert declared['additionalProperties'] is False
    assert set(issue['constraints']['allowed_fields']) == set(declared['properties'])


NUMERIC = [(pipeline, path, constraint, wrong) for pipeline, candidate in BASES.items()
           for path, constraint, _, wrong in field_cases(SCHEMAS[pipeline], candidate)
           if constraint in {'minimum', 'maximum'}]


@pytest.mark.parametrize('pipeline,path,constraint,wrong', NUMERIC)
def test_all_numeric_bounds_report_rejected_value(pipeline, path, constraint, wrong):
    candidate = deepcopy(BASES[pipeline])
    at(candidate, path[:-1])[path[-1]] = wrong
    assert any(issue.get('actual_value') == wrong and constraint in issue.get('constraints', {})
               for issue in errors_for(pipeline, candidate))


@pytest.mark.parametrize('duplicate', [False, True])
def test_emotion_shapes_report_neutral_and_unique_rules_without_crashing(duplicate):
    schema_version = extraction_schema.SCHEMA_VERSION
    candidate = complete_candidate()
    tags = ['trust', 'trust'] if duplicate else ['neutral', 'trust']
    candidate['node_changes'][0]['emotion']['tags'] = [{'tag': tag, 'intensity': .4} for tag in tags]
    issue = regeneration.validation_feedback(archive(candidate), schema_version=schema_version)['errors'][0]
    if duplicate:
        assert issue['type'] == 'duplicate_values'
        assert issue['violations'][0]['actual_items'] == 2
        assert issue['violations'][0]['distinct_items'] == 1
    else:
        assert issue['type'] == 'neutral_emotion_conflict'
        assert issue['actual_items'] == 2
        assert issue['constraints'] == {'when_neutral_present_maxItems': 1}


def dynamic(candidate, code, path, context, schema=extraction_schema.SCHEMA_VERSION):
    return regeneration.validation_feedback(archive(candidate, code=code, path=path),
        schema_version=schema, context=context)['errors'][0]


def fixed_context():
    return {'space': {'logical_user_id': 'u1'}, 'known_nodes': [
        {'node_id': 'n1', 'node_type': 'event', 'content_text': '旧正文', 'content_version': 4,
         'emotion': {'tags': [{'tag': 'trust', 'intensity': .4}, {'tag': 'joy', 'intensity': .4}], 'description': '愿意陪伴。'}},
        {'node_id': 'n2', 'node_type': 'atomic', 'content_text': '旧细节', 'content_version': 2}],
        'known_profiles': [{'logical_user_id': 'u1', 'content_version': 3}],
        'known_relationship': {'logical_user_id': 'u1', 'content_version': 5},
        'sources': [{'source_id': 's1', 'actor_kind': 'user', 'actor_logical_user_id': 'u1'},
                    {'source_id': 's2', 'actor_kind': 'user', 'actor_logical_user_id': 'u2'},
                    {'source_id': 's3', 'actor_kind': 'agent'}]}


@pytest.mark.parametrize('kind,expected', [('node', 4), ('profile', 3), ('relationship', 5)])
def test_versions_are_filled_from_fixed_input(kind, expected):
    candidate, context = complete_candidate(), fixed_context()
    candidate['node_changes'][0].update(operation='update', node_id='n1', expected_version=99)
    parsed = extraction_schema.parse_extraction_output_for_context(
        json.dumps(candidate), context, validate_supplied_versions=False).model_dump(mode='json')
    path = {'node': ('node_changes', 0), 'profile': ('profile_changes', 0),
            'relationship': ('relationship_change',)}[kind]
    item = parsed
    for component in path:
        item = item[component]
    assert item['expected_version'] == expected


@pytest.mark.parametrize('change,expected', [('new', 1), ('title', 4), ('content', 5), ('emotion', 5), ('order', 4), ('unchanged', 4), ('whitespace', 5), ('emotion_whitespace', 4)])
def test_coverage_versions_match_content_and_normalized_emotion(change, expected):
    candidate, context = complete_candidate(), fixed_context()
    node = candidate['node_changes'][0]
    old = context['known_nodes'][0]
    if change != 'new':
        node.update(operation='update', node_id='n1', expected_version=4, content=old['content_text'], emotion=deepcopy(old['emotion']))
    if change == 'content':
        node['content'] = '有新增细节的正文'
    elif change == 'emotion':
        node['emotion']['tags'][0]['intensity'] = .8
    elif change == 'order':
        node['emotion']['tags'].reverse()
    elif change == 'whitespace':
        node['content'] = '  ' + old['content_text'] + '\n'
    elif change == 'emotion_whitespace':
        node['emotion']['description'] = '\t' + node['emotion']['description'] + '  '
    elif change == 'title':
        node['title'] = '新标题'
    parsed = extraction_schema.parse_extraction_output_for_context(
        json.dumps(candidate), context, validate_supplied_versions=False).model_dump(mode='json')
    assert parsed['coverage_changes'][0]['expected_event_version'] == expected
    from core.memory_emotions import encode_emotion
    parsed = extraction_schema.parse_extraction_output(json.dumps(candidate))
    stored = None if change == 'new' else dict(old, emotion_json=encode_emotion(old['emotion']))
    _, changed = extraction_store._node_content_change(parsed.node_changes[0], stored)
    assert expected == (1 if stored is None else stored['content_version'] + int(changed))


def test_evidence_explains_other_user_exclusion_and_preserves_repeated_hint():
    candidate, context = complete_candidate(), fixed_context()
    candidate['profile_changes'][0]['source_ids'] = ['s1', 's2', 's3']
    saved = archive(candidate, code='evidence_user_mismatch', path='$.profile_changes[0].source_ids')
    saved['window']['response_diagnostics'] = {'repeated_candidate': True}
    issue = regeneration.validation_feedback(saved, context=context)['errors'][0]
    assert issue['constraints'] == {'minimum_own_user_sources': 1, 'maximum_other_user_sources': 0}
    assert issue['actual_own_user_sources'] == issue['actual_other_user_sources'] == 1
    assert '完全相同' in issue['instruction'] and '所有user来源' in issue['instruction']


@pytest.mark.parametrize('path,key,expected', [
    ('$.profile_changes[0].logical_user_id', 'enum', ['u1', 'u2', 'u3', 'u4']),
    ('$.node_changes[0].content', 'allowed_user_ids', ['u1', 'u2', 'u3', 'u4']),
    ('$.node_changes[0].source_ids[0]', 'enum', ['s1', 's2', 's3']),
    ('$.node_changes[0].base_node_ids[0]', 'enum', ['n1', 'n2']),
])
def test_reference_domains_use_all_fixed_identities_without_draft_ids(path, key, expected):
    candidate, context = complete_candidate(), fixed_context()
    context['sources'][2]['conversation_logical_user_id'] = 'u3'
    context['known_nodes'][0]['content_text'] = '{{user:u4}}提过的事'
    candidate['profile_changes'][0]['logical_user_id'] = 'u999'
    issue = dynamic(candidate, 'unknown_reference', path, context)
    assert issue['constraints'][key] == expected


def test_initial_summary_evidence_and_topic_create_minimum_are_measured():
    summary = {**SUMMARY, 'evidence_source_ids': []}
    issue = errors_for('summary', summary)[0]
    assert issue['constraints']['minItems'] == 1 and issue['actual_items'] == 0
    topic = deepcopy(TOPIC)
    topic['changes'][0].update(operation='create', topic_id=None, expected_version=None, member_ids=['n1'])
    issue = errors_for('topic', topic)[0]
    assert issue['constraints']['minItems'] == topic_schema.TOPIC_CREATE_MIN_MEMBERS
    assert issue['actual_items'] == issue['distinct_items'] == 1


def test_topic_update_domain_is_fixed_and_protection_is_not_recoverable():
    candidate = deepcopy(TOPIC)
    candidate['changes'][0]['expected_version'] = 99
    context = {'nodes': [{'node_id': 't1', 'node_type': 'topic', 'content_version': 4, 'status': 'active'},
                         {'node_id': 't2', 'node_type': 'topic', 'content_version': 2, 'status': 'active', 'manual_protected': True}]}
    issues = feedback.validation_errors(feedback.CandidateValidationError('topic_update_identity', ('changes', 0), ''),
        json.dumps(candidate), SCHEMAS['topic'], topic_regeneration.EXPLANATIONS, context=context, pipeline='topic')
    assert issues[0]['constraints']['topic_id']['enum'] == ['t1']
    assert 'expected_version' not in issues[0]['constraints']
    for code in ['protected', 'context_changed', 'configuration_changed', 'lease_lost', 'output_capacity_exceeded']:
        assert not feedback.correction_allowed([{'type': code}], topic_regeneration.EXPLANATIONS)
        assert not regeneration.automatic_correction_allowed('invalid_output', {'validation_errors': [{'type': code}]})


# Every named hard rule must be deliberately classified here. Numeric/cross-field
# rules have structured metrics; format/runtime rules cannot invent a numeric fix.
STRUCTURED = {
    'readonly_node', 'topic_protected_membership', 'topic_locked_membership',
    'duplicate_values', 'unknown_event_time', 'event_time_interval', 'create_identity_invalid',
    'update_identity_required', 'node_content_too_long', 'neutral_emotion_conflict',
    'duplicate_edges', 'unknown_local_reference', 'edge_self_reference', 'source_sets_overlap',
    'event_metadata_mismatch', 'result_changes_mismatch', 'coverage_self_reference', 'duplicate_coverage_pair',
    'source_outside_window', 'evidence_user_mismatch', 'node_outside_context', 'node_type_mismatch',
    'expected_node_version_mismatch', 'profile_user_mismatch', 'group_profile_limit',
    'expected_profile_version_mismatch', 'relationship_user_mismatch', 'expected_relationship_version_mismatch',
    'coverage_endpoint_type_mismatch', 'coverage_event_version_mismatch', 'coverage_atomic_version_mismatch',
    'invalid_contains_hierarchy', 'supersedes_node_type_mismatch', 'supersedes_cycle',
    'event_identity_mismatch', 'profile_evidence_insufficient', 'profile_repeated_evidence_missing',
    'interaction_confirmation_required', 'interaction_confirmation_invalid',
    'topic_create_identity', 'topic_create_members', 'ordinary_topic_create_members', 'topic_update_identity', 'topic_duplicate_members',
    'topic_duplicate_update', 'topic_member_set', 'topic_member_unavailable', 'topic_unknown_user',
    'summary_unknown_user', 'initial_summary_evidence_required', 'relationship_slot_body', 'event_content_insufficient',
}
FORMAT_OR_PROTOCOL = {'invalid_user_reference', 'schema_mismatch', 'json_invalid'}
HARD_STOP = {'output_capacity_exceeded'}


def test_every_named_standard_has_an_explicit_metrics_or_nonnumeric_classification():
    declared = set()
    for module in (extraction_schema, extraction_store, topic_schema, topic_store, short_term_prompts):
        for node in ast.walk(ast.parse(inspect.getsource(module))):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {
                'PydanticCustomError', 'CandidateValidationError', 'InvalidExtractionCandidate', 'fail'}:
                if node.args and isinstance(node.args[0], ast.Constant):
                    declared.add(node.args[0].value)
    assert declared <= STRUCTURED | FORMAT_OR_PROTOCOL | HARD_STOP | feedback.FIELD_RULES.keys()
    # New named structured rules must be implemented, not just put in an allowlist.
    implemented = {node.value for node in ast.walk(ast.parse(inspect.getsource(feedback.enrich_feedback) + inspect.getsource(feedback._fixed_context_feedback) + inspect.getsource(regeneration._group_profile_feedback)))
                   if isinstance(node, ast.Constant) and isinstance(node.value, str)}
    assert STRUCTURED <= implemented


@pytest.mark.parametrize('node_type', [[], {}])
def test_multiple_wrong_field_types_still_return_independent_errors(node_type):
    candidate = complete_candidate()
    candidate['node_changes'][0].update(node_type=node_type, content=42)
    errors = errors_for('extraction', candidate)
    assert {(error['path'], error['type']) for error in errors} == {
        ('$.node_changes[0].node_type', 'literal_error'),
        ('$.node_changes[0].content', 'string_type'),
    }
    assert all(error['constraints'] for error in errors)
