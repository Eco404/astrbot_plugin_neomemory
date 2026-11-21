"""Deterministic capacity comparison, not evidence of model attention or quality.

Synthetic ceiling-sized prose exercises production whole-entry assembly. Real
generated relationship lengths are measured separately by the model experiment.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from core.config import NeoMemoryConfig
from core.context_bundle import assemble_context
from core.relationship_state import relationship_slots, relationship_text


def fill(text, length):
    return (text * (length // len(text) + 1))[:length]


def evaluate():
    hits = [{"node_id": f"synthetic-event-{index}", "node_type": "event", "status": "active",
             "content_text": fill("观测安排需晴天进行，报名确认后才能参加。", 750)}
            for index in range(6)]
    result = {"synthetic_only": True, "method": "Identical six ranked event candidates and configuration; "
              "only relationship body lengths vary. Ceiling stress, not natural output or semantic evaluation.",
              "cases": []}
    for label, lengths in (("v8_ceiling", (1600, 600)), ("v9_ceiling", (500, 150))):
        state = relationship_slots()
        for slot, size, text in (("long_term", lengths[0], "我愿意靠近，也保留自己的步调。"),
                                 ("aftereffect", lengths[1], "这次交谈让我放松，也还有些犹豫。")):
            state[slot] = {"body": fill(text, size), "observed_at": 100}
        snapshot = {"relationship": {"state": state, "state_text": relationship_text(state)},
                    "profile": None, "short_term": None, "active_notes": 0}
        for auxiliary in (False, True):
            current = dict(snapshot)
            if auxiliary:
                current.update(profile={"tags": ["天文"], "description": fill("观测活动需提前安排。", 300)},
                               short_term={"summary_text": fill("下次活动时间尚待确认。", 600)})
            config = NeoMemoryConfig.from_mapping({"recall_budget_tokens": 15000})
            bundle = assemble_context(current, hits, config, now=101)
            included = [entry for entry in bundle.diagnostics["entries"] if entry["included"]]
            relationship_bytes = sum(entry["rendered_bytes"] for entry in included
                                     if entry["section"] in {"relationship", "relationship_aftereffect"})
            result["cases"].append({"arm": label, "auxiliary_context": auxiliary,
                "body_chars": list(lengths), "relationship_bytes_excluding_shared_envelope": relationship_bytes,
                "relationship_fraction_of_budget": round(relationship_bytes / 15000, 4),
                "long_term_nodes": len(bundle.hits), "long_term_bytes": bundle.diagnostics["long_term_bytes"],
                "injected_bytes": bundle.diagnostics["injected_bytes"],
                "remaining_bytes": bundle.diagnostics["remaining_bytes"],
                "omitted": bundle.diagnostics["long_term_omitted"]})
            assert bundle.diagnostics["injected_bytes"] <= 15000
            assert all(state[slot]["body"] in bundle.text for slot in ("long_term", "aftereffect"))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = evaluate()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))
