"""Check fixture identity and evidence boundaries before any paid evaluation."""

import hashlib
import json

import pytest

from scripts.fidelity_cases import (
    FIDELITY_CASESET_VERSION,
    case_manifest,
    case_set_digest,
    fidelity_cases,
    synthetic_persona,
)


def test_matrix_has_six_development_and_two_frozen_nonblind_windows():
    cases = fidelity_cases()
    assert len(cases) == 8
    assert [case["split"] for case in cases] == ["dev"] * 6 + ["frozen"] * 2
    assert len({case["id"] for case in cases}) == 8
    assert [case["evaluation_visibility"] for case in cases[-2:]] == ["frozen_not_blind"] * 2
    assert case_manifest()["frozen_evaluation_is_blind"] is False


def test_only_continuous_pair_reuses_private_identity_and_has_prior_dependency():
    seen = {}
    private_speakers = {}
    for case in fidelity_cases():
        dependency = case["depends_on"]
        if dependency:
            assert dependency in seen  # A missing/cyclic or later dependency cannot be replayed.
            assert seen[dependency]["split"] == case["split"]
        speakers = {message["speaker"] for message in case["messages"] if message["actor"] == "user"}
        if not case["group"]:
            assert len(speakers) == 1
            speaker = next(iter(speakers))
            if speaker in private_speakers:
                assert dependency == private_speakers[speaker]
            else:
                assert dependency is None
            private_speakers[speaker] = case["id"]
        seen[case["id"]] = case
    assert [(case["id"], case["depends_on"]) for case in seen.values() if case["depends_on"]] == [
        ("habit_revision", "habit_initial"),
    ]


@pytest.mark.parametrize("case", fidelity_cases(), ids=lambda case: case["id"])
def test_each_window_has_explicit_roles_and_complete_source_questions(case):
    for message in [*case["messages"], *case["seed_messages"]]:
        assert set(message) == {"actor", "speaker", "text"}
        assert message["actor"] in {"user", "agent"}
        assert message["speaker"].strip() and message["text"].strip()
    users = {message["speaker"] for message in case["messages"] if message["actor"] == "user"}
    agents = {message["speaker"] for message in case["messages"] if message["actor"] == "agent"}
    assert users.isdisjoint(agents)
    requirements = case["requirements"]
    assert requirements
    assert len({item["id"] for item in requirements}) == len(requirements)
    for requirement in requirements:
        assert {"id", "field", "question", "expected_answer"} <= set(requirement)
        assert set(requirement) <= {"id", "field", "question", "expected_answer", "applicability"}
        assert all(isinstance(value, str) and value.strip() for value in requirement.values())
    assert case["expected"]["profile_expected"] in (None, True, False)
    assert isinstance(case["expected"]["required_retained"], list)
    assert case["expected"]["forbidden_inferences"]
    assert case["allowed"]["alternative_expressions"]
    assert case["allowed"]["node_granularity"]
    assert case["allowed"]["coverage"]


def test_node_history_requirement_is_conditional_on_a_prior_node():
    requirements = [
        (case["id"], item) for case in fidelity_cases() for item in case["requirements"]
        if "applicability" in item
    ]
    assert len(requirements) == 1
    case_id, item = requirements[0]
    assert case_id == "habit_revision" and item["id"] == "node_history"
    assert item["applicability"] == "prior_repair_node"
    assert "不要求本窗新建" in item["expected_answer"]
    assert "不作为质量失败" in item["expected_answer"]
    assert "不宣称已验证历史保真" in item["expected_answer"]


def test_seeded_coverage_uses_independent_historical_sources_and_stable_endpoints():
    seeded = [case for case in fidelity_cases() if case["seed_nodes"]]
    assert len(seeded) == 3
    all_refs = []
    for case in fidelity_cases():
        assert bool(case["seed_messages"]) == bool(case["seed_nodes"])
    for case in seeded:
        assert case["depends_on"] is None
        assert {node["type"] for node in case["seed_nodes"]} == {"atomic", "event"}
        assert len(case["seed_nodes"]) == 2
        current_texts = {message["text"] for message in case["messages"]}
        assert current_texts.isdisjoint(message["text"] for message in case["seed_messages"])
        for node in case["seed_nodes"]:
            all_refs.append(node["ref"])
            assert node["content"].strip()
            assert node["source_indices"]
            assert len(set(node["source_indices"])) == len(node["source_indices"])
            for index in node["source_indices"]:
                assert type(index) is int and 0 <= index < len(case["seed_messages"])
                source = case["seed_messages"][index]
                assert source["actor"] == "user"
                assert source["speaker"] == case["messages"][0]["speaker"]
        question_ids = {question["id"] for question in case["requirements"]}
        assert {"coverage_truth", "coverage_versions", "event_identity"} <= question_ids
    assert len(set(all_refs)) == len(all_refs)


def test_low_value_group_does_not_require_nonempty_memory():
    groups = [case for case in fidelity_cases() if case["group"]]
    assert len(groups) == 1
    group = groups[0]
    assert group["expected"]["profile_expected"] is False
    assert not group["expected"]["required_retained"]
    assert not group["seed_nodes"]
    assert len({message["speaker"] for message in group["messages"] if message["actor"] == "user"}) == 2
    assert {question["id"] for question in group["requirements"]} >= {
        "legitimate_no_change", "group_identity", "group_relationship",
    }


def test_manifest_binds_persona_sources_old_state_and_review_standards():
    manifest = case_manifest()
    assert manifest["version"] == FIDELITY_CASESET_VERSION
    assert manifest["provenance"] == "independently_authored_synthetic"
    assert manifest["persona"] == synthetic_persona()
    encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    baseline = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    assert case_set_digest() == baseline
    mutations = [
        lambda data: data.update(persona="different synthetic persona"),
        lambda data: data["cases"][0]["messages"][0].update(actor="agent"),
        lambda data: data["cases"][0]["requirements"][0].update(expected_answer="changed rubric"),
        lambda data: data["cases"][2]["seed_nodes"][0].update(content="changed historical state"),
        lambda data: data["cases"][-1].update(split="dev"),
    ]
    for mutation in mutations:
        changed = case_manifest()
        mutation(changed)
        encoded = json.dumps(changed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        assert hashlib.sha256(encoded.encode("utf-8")).hexdigest() != baseline
    assert case_set_digest() == baseline  # No shared mutable fixture survives a caller's edits.


def test_review_material_is_separate_from_model_source_content():
    for case in fidelity_cases():
        source_input = json.dumps({
            "messages": case["messages"],
            "seed_messages": case["seed_messages"],
            "seed_nodes": case["seed_nodes"],
        }, ensure_ascii=False)
        for field in ("expected_answer", "requirements", "required_retained", "forbidden_inferences"):
            assert field not in source_input
        for question in case["requirements"]:
            assert question["question"] not in source_input
            assert question["expected_answer"] not in source_input
