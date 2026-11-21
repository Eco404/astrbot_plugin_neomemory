"""Deterministic lexical audit for synthetic persona-voice evaluations.

This is a review aid, not a semantic judge. It reports likely violations and
never rewrites a candidate or turns a lexical count into a quality claim.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable


MEMORY_MANAGEMENT_PHRASES = (
    "我记得",
    "我会记得",
    "我提过",
    "我们之前说过",
    "我知道",
    "我会记住",
)

# This list deliberately stays small. A hit is a review signal, not proof of
# a subject error, because the name can be a quote, title, or proper noun.
PERSONA_SUBJECT_VERBS = (
    "表示",
    "答应",
    "认为",
    "说道",
    "说",
    "喜欢",
    "担心",
    "高兴",
    "想",
    "陪",
    "正在",
    "决定",
    "记得",
)


def _text_values(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _text_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from _text_values(child)


def generated_texts(payload: dict[str, Any]) -> list[str]:
    """Return only generated prose fields, excluding source and IDs."""
    texts: list[str] = []
    for key in ("node_changes", "profile_changes"):
        for item in payload.get(key, ()):
            for field in ("title", "content", "description", "tags"):
                texts.extend(_text_values(item.get(field)))
            emotion = item.get("emotion")
            if isinstance(emotion, dict):
                texts.extend(_text_values(emotion.get("description")))
    relationship = payload.get("relationship_change")
    if isinstance(relationship, dict):
        texts.extend(_text_values(relationship.get("state_text")))
    return texts


def audit_payload(payload: dict[str, Any], *, persona_name: str | None = None) -> dict[str, Any]:
    """Produce conservative aggregate signals for one canonical candidate."""
    texts = generated_texts(payload)
    joined = "\n".join(texts)
    management_hits = sum(joined.count(phrase) for phrase in MEMORY_MANAGEMENT_PHRASES)
    generic_user_hits = joined.count("用户")
    placeholder_hits = len(re.findall(r"\{\{user:[^{}]+\}\}", joined))
    relationship = payload.get("relationship_change")
    relationship_text = relationship.get("state_text", "") if isinstance(relationship, dict) else ""
    third_person_hits = 0
    if persona_name:
        pattern = re.compile(re.escape(persona_name) + r"(?:的)?(?:" + "|".join(PERSONA_SUBJECT_VERBS) + r")")
        third_person_hits = len(pattern.findall(joined))
    return {
        "text_field_count": len(texts),
        "memory_management_phrase_hits": management_hits,
        "generic_user_hits": generic_user_hits,
        "user_placeholder_hits": placeholder_hits,
        "persona_name_subject_hits": third_person_hits,
        "relationship_present": isinstance(relationship, dict),
        "relationship_first_person_signal": bool(relationship_text and "我" in relationship_text),
        "requires_manual_semantic_review": True,
    }


def _persona_name(persona: str) -> str | None:
    match = re.search(r"我是([^，。！？\s]+)", persona)
    return match.group(1) if match else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="narrative evaluation directory")
    args = parser.parse_args()
    outputs = []
    for input_path in sorted(args.directory.glob("*/input.json")):
        case_dir = input_path.parent
        candidate_path = case_dir / "canonical.json"
        if not candidate_path.is_file():
            continue
        input_data = json.loads(input_path.read_text(encoding="utf-8"))
        payload = json.loads(candidate_path.read_text(encoding="utf-8"))
        case = input_data.get("case", {})
        metrics = audit_payload(payload, persona_name=_persona_name(case.get("persona", "")))
        output = {"case": case.get("name", case_dir.name), **metrics}
        (case_dir / "voice_metrics.json").write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        outputs.append(output)
    print(json.dumps({"cases": outputs, "semantic_review_required": True}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
