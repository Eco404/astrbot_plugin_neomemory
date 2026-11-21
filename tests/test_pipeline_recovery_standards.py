"""Declared schema constraints and named rules must have usable recovery feedback."""
import ast
from copy import deepcopy
import inspect
import json
import re

import pytest

from core import candidate_feedback, short_term_prompts, topic_schema, topic_regeneration
from core import extraction_regeneration, extraction_schema
from core.short_term import RECOVERY_EXPLANATIONS
from storage import topic_store
from core.text_lengths import readable_text_length
from tests.test_regeneration_contract import complete_candidate, field_cases

TOPIC = {'schema_version': topic_schema.VERSION, 'changes': [{
    'operation': 'update', 'topic_id': 't1', 'expected_version': 1,
    'title': '独立合成活动', 'content': '两次活动仍在准备，只有晴天才出发。',
    'importance': .4, 'emotion': {'tags': [{'tag': 'trust', 'intensity': .4}], 'description': '我愿意协助准备。'},
    'member_ids': ['n1', 'n2'],
}]}
SUMMARY = {'schema_version': short_term_prompts.PROMPT_VERSION,
           'summary_text': '只有晴天才出发，现在尚未出发。', 'evidence_source_ids': ['s1']}
SCHEMAS = {
    'topic': topic_schema.output_schema(),
    'summary': short_term_prompts.output_schema(source_ids=['s1', 's2'], summary_chars=400),
}
BASELINES = {'topic': TOPIC, 'summary': SUMMARY}
CASES = [(pipeline, *item) for pipeline in BASELINES
         for item in field_cases(SCHEMAS[pipeline], BASELINES[pipeline])]
LENGTH_CONSTRAINTS = {'minLength', 'maxLength', 'x-readable-maxLength', 'minItems', 'maxItems'}


def assert_actual(errors, path, constraint, wrong):
    expected_path = '$' + ''.join(f'[{part}]' if type(part) is int else '.' + part for part in path)
    key = ('actual_readable_chars' if constraint == 'x-readable-maxLength'
           else 'actual_encoded_chars' if isinstance(wrong, str) else 'actual_items')
    expected = readable_text_length(wrong) if key == 'actual_readable_chars' else len(wrong)
    assert any(error.get('path') == expected_path and error.get(key) == expected for error in errors), errors


@pytest.mark.parametrize('pipeline,path,constraint,field,wrong', CASES,
    ids=[f'{pipeline}:' + '.'.join(map(str, path)) + ':' + constraint
         for pipeline, path, constraint, _, _ in CASES])
def test_each_declared_field_constraint_rejects_and_explains(pipeline, path, constraint, field, wrong):
    value = deepcopy(BASELINES[pipeline])
    owner = value
    for key in path[:-1]:
        owner = owner[key]
    owner[path[-1]] = wrong
    raw = json.dumps(value, ensure_ascii=False)
    with pytest.raises(ValueError) as failure:
        if pipeline == 'topic':
            topic_schema.parse_output(raw)
        else:
            short_term_prompts.parse_output(raw, source_ids=['s1', 's2'], summary_chars=400)
    explanations = topic_regeneration.EXPLANATIONS if pipeline == 'topic' else RECOVERY_EXPLANATIONS
    errors = candidate_feedback.validation_errors(failure.value, raw, SCHEMAS[pipeline], explanations)
    if errors[0]['type'] == 'output_capacity_exceeded':
        assert not candidate_feedback.correction_allowed(errors, explanations)
    else:
        assert candidate_feedback.correction_allowed(errors, explanations)
        if constraint != 'type':
            expected_path = '$' + ''.join(f'[{part}]' if type(part) is int else '.' + part for part in path)
            assert any(error['path'] == expected_path
                       and error.get('constraints', {}).get(constraint) == field[constraint] for error in errors)
        if constraint in LENGTH_CONSTRAINTS:
            assert_actual(errors, path, constraint, wrong)


EXTRACTION_LENGTH_CASES = [case for case in field_cases() if case[1] in LENGTH_CONSTRAINTS]


@pytest.mark.parametrize('path,constraint,field,wrong', EXTRACTION_LENGTH_CASES,
    ids=['.'.join(map(str, path)) + ':' + constraint
         for path, constraint, _, _ in EXTRACTION_LENGTH_CASES])
def test_extraction_lengths_report_actual_value_at_rejected_path(path, constraint, field, wrong):
    value = complete_candidate()
    owner = value
    for part in path[:-1]:
        owner = owner[part]
    owner[path[-1]] = wrong
    errors = extraction_regeneration.validation_feedback({
        'error_code': 'invalid_output', 'window': {'raw': json.dumps(value, ensure_ascii=False)},
    })['errors']
    if errors[0]['type'] == 'output_capacity_exceeded':
        assert not extraction_regeneration.automatic_correction_allowed('invalid_output', {'validation_errors': errors})
    else:
        assert_actual(errors, path, constraint, wrong)


@pytest.mark.parametrize('pipeline', ['extraction', 'topic', 'summary'])
def test_identity_references_use_declared_length_unit_in_feedback(pipeline):
    reference = '{{user:independent_synthetic_identity}}'
    text = reference + '字' * (60 if pipeline != 'summary' else 400)
    if pipeline == 'extraction':
        value = complete_candidate()
        value['node_changes'][0]['title'] = text
        errors = extraction_regeneration.validation_feedback({
            'error_code': 'invalid_output', 'window': {'raw': json.dumps(value)},
        })['errors']
        path = ('node_changes', 0, 'title')
    else:
        value = deepcopy(BASELINES[pipeline])
        if pipeline == 'topic':
            value['changes'][0]['title'] = text
            path = ('changes', 0, 'title')
        else:
            value['summary_text'] = text
            path = ('summary_text',)
        raw = json.dumps(value)
        with pytest.raises(ValueError) as failure:
            if pipeline == 'topic':
                topic_schema.parse_output(raw)
            else:
                short_term_prompts.parse_output(raw, source_ids=['s1', 's2'], summary_chars=400)
        errors = candidate_feedback.validation_errors(failure.value, raw, SCHEMAS[pipeline],
            topic_regeneration.EXPLANATIONS if pipeline == 'topic' else RECOVERY_EXPLANATIONS)
    assert_actual(errors, path, 'maxLength' if pipeline == 'summary' else 'x-readable-maxLength', text)


@pytest.mark.parametrize('module,explanations,excluded', [
    (topic_schema, topic_regeneration.EXPLANATIONS, set()),
    (topic_store, topic_regeneration.EXPLANATIONS, set()),
    (short_term_prompts, RECOVERY_EXPLANATIONS, {'output_capacity_exceeded'}),
])
def test_all_declared_candidate_codes_have_explicit_classification(module, explanations, excluded):
    # This cross-module invariant catches a new hard rule lacking aligned
    # feedback. It does not assert that every runtime exception is recoverable.
    declared = set()
    for node in ast.walk(ast.parse(inspect.getsource(module))):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in {'PydanticCustomError', 'CandidateValidationError'}):
            assert isinstance(node.args[0], ast.Constant), 'Declare a stable error code for each fixed rule'
            declared.add(node.args[0].value)
    assert declared
    for code in declared:
        assert candidate_feedback.correction_allowed([{'type': code}], explanations) == (code not in excluded), code


def test_all_extraction_named_schema_rules_have_explicit_feedback():
    declared = {node.args[0].value for node in ast.walk(ast.parse(inspect.getsource(extraction_schema)))
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == 'PydanticCustomError' and isinstance(node.args[0], ast.Constant)}
    assert declared <= candidate_feedback.FIELD_RULES.keys() | extraction_regeneration._EXPLANATIONS.keys()


@pytest.mark.parametrize('text', ['', ' ', '\t\n', '\u3000', '等待安排', '  等待安排  '])
def test_summary_schema_and_parser_agree_on_empty_text_evidence(text):
    schema = short_term_prompts.output_schema(source_ids=['s1'], summary_chars=400)
    condition = schema['allOf'][0]['if']['properties']['summary_text']
    schema_requires_evidence = re.search(condition['pattern'], text) is not None
    raw = json.dumps({'schema_version': short_term_prompts.PROMPT_VERSION,
                     'summary_text': text, 'evidence_source_ids': []})
    if schema_requires_evidence:
        with pytest.raises(candidate_feedback.CandidateValidationError) as error:
            short_term_prompts.parse_output(raw, source_ids=['s1'], summary_chars=400)
        assert error.value.code == 'initial_summary_evidence_required'
    else:
        result = short_term_prompts.parse_output(raw, source_ids=['s1'], summary_chars=400)
        assert result['summary_text'] == ''
