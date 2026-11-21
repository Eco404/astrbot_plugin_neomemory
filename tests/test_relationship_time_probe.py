"""Keep paired reader inputs identical except for the frozen time treatment."""
import copy
import json

from scripts import probe_relationship_time as probe


def test_paired_reader_inputs_and_hidden_review_material():
    for case in probe.CASES:
        before = copy.deepcopy(case)
        payloads = [json.loads(probe.messages_for(case, branch)[1]['content']) for branch in probe.BRANCHES]
        for payload in payloads:
            assert set(payload) == {'persona', 'memory', 'current_time_utc', 'message'}
            assert payload['persona'] == case['persona']
            assert payload['message'] == case['question']
            assert payload['current_time_utc'] == case['now']
            assert payload['memory'].count(case['body']) == 1
        plain = payloads[0]['memory']
        assert payloads[1]['memory'] == plain + '\n' + probe.NOTE
        assert payloads[2]['memory'].startswith(payloads[1]['memory'] + '\n')
        assert case == before


def test_unknown_does_not_fall_back_to_current_time_and_spec_is_stable():
    case = next(case for case in probe.CASES if case['name'] == 'unknown')
    rendered = probe.memory_for(case, 'source_time')
    assert rendered.endswith('未知')
    assert case['now'] not in rendered
    spec = probe.specification()
    assert spec == probe.specification()
    assert spec['calls'] == 36 and spec['retries'] == 0
    assert len({case['persona'] for case in spec['cases']}) == 2
