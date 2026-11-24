"""Every declared field constraint has aligned validation and recovery feedback."""

from copy import deepcopy
import json

from pydantic import ValidationError
import pytest

from core.extraction_errors import ExtractionCapacityError
from core.extraction_regeneration import (
    automatic_correction_allowed, validation_feedback,
)
from core.extraction_schema import SCHEMA_VERSION, extraction_json_schema, extraction_model_json_schema, parse_extraction_output
from tests.test_maintenance_engine import node, output


def complete_candidate(schema_version=SCHEMA_VERSION):
    event = node('s1', title='合成设备维修', content='只有仪器到货才开始，尚未开工。')
    event.update(event_time={'start': 10., 'end': 20., 'text': '合成时间范围'},
        emotion={'tags': [{'tag': 'concern', 'intensity': .3}], 'description': '我担心准备不足。'},
        base_node_ids=[])
    atomic = {**node('s1'), 'node_type': 'atomic', 'local_ref': 'local_atomic', 'base_node_ids': []}
    result = output([event, atomic],
        profiles=[{'logical_user_id': 'u1', 'expected_version': 0, 'tags': ['repair'],
                   'description': '喜欢维修。', 'source_ids': ['s1']}],
        relationship={'logical_user_id': 'u1', 'expected_version': 0, 'source_ids': ['s1'],
                      'long_term': {'op': 'replace', 'body': '我愿意耐心陪伴。'},
                      'aftereffect': {'op': 'replace', 'body': '这次交流让我有些期待。'}},
        edges=[{'from_ref': event['local_ref'], 'to_ref': 'local_atomic', 'relation_type': 'contains', 'source_ids': ['s1']}],
        coverage_changes=[{'event_ref': event['local_ref'], 'atomic_ref': 'local_atomic',
            'expected_event_version': 1, 'expected_atomic_version': 1, 'coverage_kind': 'partial', 'source_ids': ['s1']}])
    result['schema_version'] = schema_version
    return result


def field_cases(schema=None, baseline=None):
    if schema is None:
        schema = extraction_model_json_schema()
        baseline = complete_candidate() if baseline is None else baseline
        for change in baseline['node_changes']:
            change.pop('expected_version', None)
        for change in baseline['profile_changes']:
            change.pop('expected_version', None)
        if baseline['relationship_change']:
            baseline['relationship_change'].pop('expected_version', None)
        for change in baseline['coverage_changes']:
            change.pop('expected_event_version', None)
            change.pop('expected_atomic_version', None)
    else:
        baseline = complete_candidate() if baseline is None else baseline

    def resolve(field):
        if '$ref' in field:
            return resolve(schema['$defs'][field['$ref'].rsplit('/', 1)[1]])
        if 'anyOf' in field:
            return resolve(next(v for v in field['anyOf'] if v.get('type') != 'null'))
        return field

    def walk(field, value, path=()):
        field = resolve(field)
        if path and path != ('schema_version',):
            if field.get('type') in {'string', 'number', 'integer', 'array', 'object'}:
                wrong = [] if field['type'] != 'array' else {}
                yield path, 'type', field, wrong
            for key in ('minLength', 'maxLength', 'minimum', 'maximum', 'minItems', 'maxItems',
                        'enum', 'const', 'pattern', 'x-readable-maxLength'):
                if key not in field:
                    continue
                bound = field[key]
                if key == 'minLength':
                    wrong = ''
                elif key == 'maxLength':
                    wrong = 'x' * (bound + 1)
                elif key == 'minimum':
                    wrong = bound - 1
                elif key == 'maximum':
                    wrong = bound + 1
                elif key == 'minItems':
                    wrong = []
                elif key == 'maxItems':
                    item = value[0] if value else 'n1'
                    wrong = [deepcopy(item) for _ in range(bound + 1)]
                elif key in {'enum', 'const'}:
                    wrong = 'unknown-enumeration'
                elif key == 'pattern':
                    wrong = ' ' * max(8, field.get('minLength', 1))
                else:
                    if path[-1] == 'content':
                        owner = baseline
                        for component in path[:-1]:
                            owner = owner[component]
                        bound = field.get('x-readable-limitsByNodeType', {}).get(owner.get('node_type'), bound)
                    wrong = '字' * (bound + 1)
                yield path, key, {**field, key: bound}, wrong
        if isinstance(value, dict):
            for name, item in value.items():
                if name not in field["properties"]:
                    continue
                yield from walk(field['properties'][name], item, (*path, name))
        elif isinstance(value, list) and value:
            yield from walk(field['items'], value[0], (*path, 0))

    return list(walk(schema, baseline))


CASES = field_cases()


@pytest.mark.parametrize('path,constraint,field,wrong', CASES,
    ids=['.'.join(map(str, path)) + ':' + constraint for path, constraint, _, _ in CASES])
def test_schema_constraint_rejection_has_aligned_correction(path, constraint, field, wrong):
    candidate = complete_candidate()
    target = candidate
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = wrong
    raw = json.dumps(candidate, ensure_ascii=False)
    try:
        parse_extraction_output(raw)
    except ExtractionCapacityError:
        feedback = validation_feedback({'error_code': 'invalid_output', 'window': {'raw': raw}})
        assert feedback['errors'][0]['type'] == 'output_capacity_exceeded'
        assert not automatic_correction_allowed('invalid_output', {'validation_errors': feedback['errors']})
        return
    except ValidationError as exc:
        issues = exc.errors(include_input=False, include_context=False, include_url=False)
    else:
        pytest.fail(f'declared constraint did not reject: {path}/{constraint}')
    assert automatic_correction_allowed('invalid_output', {'validation_errors': issues})
    feedback = validation_feedback({'error_code': 'invalid_output', 'window': {'raw': raw}})
    assert all(error['type'] != 'schema_invalid' for error in feedback['errors'])
    if constraint != 'type':
        assert any(error.get('constraints', {}).get(constraint) == field[constraint] for error in feedback['errors'])
    if constraint in {'minLength', 'maxLength', 'x-readable-maxLength', 'minItems', 'maxItems'}:
        assert any(key in error for error in feedback['errors']
                   for key in ('actual_readable_chars', 'actual_encoded_chars', 'actual_items'))


@pytest.mark.parametrize('field', ['node_changes', 'profile_changes', 'relationship_change'])
def test_missing_required_change_field_is_reported_without_guessing_value(field):
    candidate = complete_candidate()
    del candidate[field]
    feedback = validation_feedback({'error_code': 'invalid_output', 'window': {'raw': json.dumps(candidate)}})
    assert feedback['errors'][0]['type'] == 'missing'
    assert automatic_correction_allowed('invalid_output', {'validation_errors': feedback['errors']})


def test_schema_version_feedback_follows_prepared_prompt_not_rejected_candidate():
    code = "schema_mismatch"
    candidate = complete_candidate()
    window = {'raw': json.dumps(candidate), 'validation_errors': [{'path': '$.schema_version', 'type': code}]}
    feedback = validation_feedback({'error_code': code, 'window': window})
    error = feedback['errors'][0]
    assert error['type'] == code
    assert '原始系统提示词' in error['instruction']
    assert 'extraction.v' not in error['instruction']
    assert automatic_correction_allowed(code, window)
