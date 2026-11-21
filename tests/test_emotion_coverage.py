"""Coverage proof versions include emotional content changed in the same batch."""

import json

import pytest

from core.memory_emotions import validate_emotion
from core.extraction_apply import ExtractionService
from core.extraction_errors import InvalidExtractionCandidate
from tests.test_emotion_extraction import emotion
from tests.test_extraction_apply import coverage, coverage_rows, node, output, prepare, stores  # noqa: F401


def candidate(changes, proof):
    return {**output(changes), "schema_version": "extraction.v1", "coverage_changes": [proof]}


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_kinds", [("event",), ("atomic",), ("event", "atomic")])
async def test_emotion_only_updates_use_final_coverage_versions_atomically(stores, changed_kinds):
    sources, memory, _ = stores
    service = ExtractionService(sources, memory)
    env = sources, memory, service
    first, source = await prepare(env)
    initial = [{**node(source.source_id, ref="local_" + kind, kind=kind, content="作品展出后有些期待。"),
                "title": "展出后的期待", "emotion": None} for kind in ("event", "atomic")]
    receipt = await service.apply(first, json.dumps(candidate(initial, coverage(source.source_id))), model_id="synthetic")
    node_ids = receipt["result"]["node_refs"]
    before = {kind: await memory.get_node(node_ids["local_" + kind]) for kind in ("event", "atomic")}
    previous_coverage = await coverage_rows(memory)
    second, source2 = await prepare(env, number=2, known=tuple(node_ids.values()))
    changes = [{**node(source2.source_id, ref="local_" + kind, kind=kind, old=before[kind],
                       content=before[kind]["content_text"]), "title": before[kind]["title"], "emotion": emotion()}
               for kind in changed_kinds]
    versions = {kind: 1 + int(kind in changed_kinds) for kind in ("event", "atomic")}
    valid_proof = coverage(source2.source_id,
        event_ref="local_event" if "event" in changed_kinds else node_ids["local_event"],
        atomic_ref="local_atomic" if "atomic" in changed_kinds else node_ids["local_atomic"],
        expected_event_version=versions["event"], expected_atomic_version=versions["atomic"])
    outdated_field = "expected_" + changed_kinds[0] + "_version"
    outdated = candidate(changes, {**valid_proof, outdated_field: 1})
    with pytest.raises(InvalidExtractionCandidate) as failure:
        await service.apply(second, json.dumps(outdated), model_id="synthetic")
    assert failure.value.code == "coverage_" + changed_kinds[0] + "_version_mismatch"
    assert await coverage_rows(memory) == previous_coverage
    assert await memory.get_extraction_batch(second.batch_id) is None
    for kind in ("event", "atomic"):
        assert await memory.get_node(node_ids["local_" + kind]) == before[kind]
        assert len(await memory.list_node_versions(node_ids["local_" + kind])) == 1

    committed = await service.apply(second, json.dumps(candidate(changes, valid_proof)), model_id="synthetic")
    assert committed["activity_round"] == second.expected_round + 1
    proof = (await coverage_rows(memory))[0]
    assert proof["event_content_version"] == versions["event"]
    assert proof["atomic_content_version"] == versions["atomic"]
    indexed = await memory.extraction_batch_index_nodes(second.batch_id)
    assert {item["node_id"] for item in indexed} == {node_ids["local_" + kind] for kind in changed_kinds}
    assert all(item["content_version"] == 2 and item["emotion"] == validate_emotion(emotion()) for item in indexed)
    third, _ = await prepare(env, number=3, known=tuple(node_ids.values()))
    known = {item["node_type"]: item for item in json.loads(third.messages()[1]["content"])["known_nodes"]}
    for kind in ("event", "atomic"):
        assert known[kind]["content_version"] == versions[kind]
        assert known[kind]["emotion"] == (validate_emotion(emotion()) if kind in changed_kinds else None)
        history = await memory.list_node_versions(node_ids["local_" + kind])
        assert history[0]["emotion"] is None
        if kind in changed_kinds:
            assert history[-1]["emotion"] == validate_emotion(emotion())
            assert history[-1]["derivations"][0]["batch_id"] == second.batch_id
