import json

from core.short_term_prompts import messages


def test_short_term_prompt_exposes_fragment_relationship_without_changing_text():
    records = [{
        "source_id": "src_1",
        "actor_kind": "user",
        "actor_account_id": "account",
        "actor_logical_user_id": "lu_1",
        "content_text": "分段内容",
        "observed_at": 1,
        "metadata_json": json.dumps({
            "source_fragment": {"parent_source_id": "src_parent", "index": 1, "count": 2}
        }),
    }]

    prompt = messages(persona="人格", previous_summary="", records=records,
                      summary_chars=400, user_ids=("lu_1",))
    payload = json.loads(prompt[1]["content"])

    assert payload["new_sources"][0]["text"] == "分段内容"
    assert payload["new_sources"][0]["source_fragment"] == {
        "parent_source_id": "src_parent", "index": 1, "count": 2
    }
    assert "不要把分段边界当成新的发言或事实" in prompt[0]["content"]
