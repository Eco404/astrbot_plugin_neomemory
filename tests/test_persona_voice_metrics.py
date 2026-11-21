import json

from scripts.persona_voice_metrics import audit_payload


def test_persona_voice_audit_reports_signals_without_rewriting_content():
    payload = {
        "node_changes": [{
            "title": "岚笺表示会陪伴",
            "content": "岚笺表示会陪{{user:u1}}看展。我记得{{user:u1}}喜欢安静的展览。",
            "emotion": {"description": "我为这次分享感到高兴"},
        }],
        "profile_changes": [{"description": "{{user:u1}}喜欢安静的展览。", "tags": ["安静"]}],
        "relationship_change": {"state_text": "我很珍惜这段交流。"},
    }
    original = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    metrics = audit_payload(payload, persona_name="岚笺")

    assert metrics["persona_name_subject_hits"] == 2
    assert metrics["memory_management_phrase_hits"] == 1
    assert metrics["generic_user_hits"] == 0
    assert metrics["user_placeholder_hits"] == 3
    assert metrics["relationship_first_person_signal"] is True
    assert metrics["requires_manual_semantic_review"] is True
    assert json.dumps(payload, ensure_ascii=False, sort_keys=True) == original
