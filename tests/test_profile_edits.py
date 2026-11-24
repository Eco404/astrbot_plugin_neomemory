from copy import deepcopy

import pytest

from core.candidate_feedback import CandidateValidationError
from core.extraction_schema import ExtractionOutput, ProfileChange
from core.profile_edits import apply_profile_edits, model_profile_schema, profile_parts


def change(user="u1", **fields):
    return {"logical_user_id": user, "tags": [], "description": "", "source_ids": ["s1"], **fields}


@pytest.mark.parametrize("text, parts", [
    ("", []),
    ("  ”Hi!”\r\n\n价格为 12.50。下一句？", ["  ”Hi!”\r\n\n", "价格为 12.50。", "下一句？"]),
    ("{{user:abc!c.d}} is a member. He runs.\nContinues", ["{{user:abc!c.d}} is a member. ", "He runs.\n", "Continues"]),
    ("只写一行\n另一行  ", ["只写一行\n", "另一行  "]),
    ("偏爱清早；不约午后；周末例外", ["偏爱清早；", "不约午后；", "周末例外"]),
    ("  \n\n先问……？！\n \n 后说。", ["  \n\n先问……？！\n \n ", "后说。"]),
    ("Alpha...!?  Beta; Gamma", ["Alpha...!?  ", "Beta; ", "Gamma"]),
    ("\r\n \t", ["\r\n \t"]),
])
def test_indexes_losslessly_reassemble_text(text, parts):
    assert profile_parts(text) == parts
    assert "".join(parts) == text


def test_schema_describes_edits_and_does_not_change_storage_schema():
    canonical = ExtractionOutput.model_json_schema()
    model = model_profile_schema(canonical)
    fields = model["$defs"]["ProfileChange"]
    for key in ("remove_tags", "remove_sentences"):
        assert key in fields["required"] and fields["properties"][key]["items"]["minimum"] == 1
        assert key not in canonical["$defs"]["ProfileChange"]["properties"]
    assert model_profile_schema(model) == model
    assert model_profile_schema({"type": "object"}) == {"type": "object"}


def test_multiuser_snapshot_target_order_and_copying_unchanged_fragments():
    frozen = [
        {"logical_user_id": "u1", "tags": ["写作", "每周投稿"], "description_sentences": [
            "喜欢写作。", "每周会约{{user:u2}}投稿。\n", "只在周末讨论选题。"]},
        {"logical_user_id": "u2", "tags": ["旅行"], "description": "只在夏天旅行。"}]
    result = [change(tags=["每月投稿", "写作"], description="每月会约{{user:u2}}投稿。",
                     remove_tags=[2], remove_sentences=[2]),
              change("u2", description="只在夏天旅行。偏爱火车。"), change("u3", description="喜欢骑车。")]
    apply_profile_edits(result, frozen)
    assert result[0] == change(tags=["写作", "每月投稿"], description="喜欢写作。每月会约{{user:u2}}投稿。\n只在周末讨论选题。")
    assert result[1] == change("u2", tags=["旅行"], description="只在夏天旅行。偏爱火车。")
    assert result[2] == change("u3", description="喜欢骑车。")
    for row in result:
        ProfileChange.model_validate(dict(expected_version=0, **row))


def test_deletion_full_rewrite_and_empty_edit_on_frozen_text():
    old = [{"logical_user_id": "u1", "tags": ["一", "二", "三"], "description": "One. Two. Three."}]
    for op, tags, desc in [
        (change(remove_tags=[2], remove_sentences=[2]), ["一", "三"], "One. Three."),
        (change(remove_tags=[2, 3], remove_sentences=[2, 3], description="Four."), ["一"], "One. Four."),
        (change(remove_tags=[1, 2, 3], remove_sentences=[1, 2, 3]), [], ""),
        (change(description="新画像。", remove_tags=[3, 1, 2], remove_sentences=[3, 1, 2]), [], "新画像。"),
        (change(), old[0]["tags"], old[0]["description"]),
    ]:
        apply_profile_edits([op], old)
        assert op == change(tags=tags, description=desc)


@pytest.mark.parametrize("fields,path,error", [
    ({"remove_tags": [1, 1]}, ("remove_tags", 1), "profile_edit_index"),
    ({"remove_sentences": [2]}, ("remove_sentences", 0), "profile_edit_index"),
    ({"remove_tags": [True]}, ("remove_tags", 0), "profile_edit_index"),
    ({"remove_tags": None}, ("remove_tags",), "list_type"),
    ({"tags": ["一", "一"]}, ("tags",), "duplicate_values"),
    ({"tags": [str(i) for i in range(12)]}, ("tags",), "profile_edit_capacity"),
    ({"description": "甲" * 600}, ("description",), "profile_edit_capacity"),
])
def test_invalid_edits_report_field_and_leave_all_changes_unmerged(fields, path, error):
    edits = [change("u2", description="首句。", remove_sentences=[]), change(**fields)]
    raw = deepcopy(edits)
    with pytest.raises(CandidateValidationError) as exc:
        apply_profile_edits(edits, [{"logical_user_id": "u1", "tags": ["一"], "description": "首句。"}])
    assert exc.value.code == error
    assert exc.value.location == ("profile_changes", 1, *path)
    assert edits == raw


def test_readable_capacity_and_group_limits_apply_to_merged_values():
    baseline = [{"logical_user_id": "u1", "tags": [str(i) for i in range(8)],
                 "description": "{{user:u2}}" * 239 + "。"}]
    edits = [change(description="新信息。", tags=["新增"])]
    with pytest.raises(CandidateValidationError) as exc:
        apply_profile_edits(edits, baseline, group=True)
    assert exc.value.location[-1] == "tags"
    assert exc.value.constraints == {"maxItems": 8, "actualItems": 9}
    edits[0]["tags"] = []
    with pytest.raises(CandidateValidationError) as exc:
        apply_profile_edits(edits, baseline, group=True)
    assert exc.value.location[-1] == "description"
    assert exc.value.constraints == {"x-readable-maxLength": 240, "actualReadableChars": 244}
    apply_profile_edits(edits, baseline)
    assert edits[0]["description"] == baseline[0]["description"] + "新信息。"


@pytest.mark.parametrize("parts,expected", [
    ([], {"maxItems": 0}), (["甲；", "乙；", "丙。"], {"minimum": 1, "maximum": 3}),
])
def test_index_feedback_matches_same_user_profile(parts, expected):
    with pytest.raises(CandidateValidationError) as exc:
        apply_profile_edits([change(remove_sentences=[4])], [
            {"logical_user_id": "u1", "description_sentences": parts, "tags": []},
            {"logical_user_id": "u2", "description_sentences": ["唯一句。"], "tags": []}])
    assert exc.value.constraints == expected


def test_edit_one_semicolon_clause_without_rewriting_adjacent_facts():
    edits = [change(remove_sentences=[2], description="改为周五；")]
    apply_profile_edits(edits, [{"logical_user_id": "u1", "description": "工作日在本地；原定周四；周日休息。"}])
    assert edits[0]["description"] == "工作日在本地；改为周五；周日休息。"
