#!/usr/bin/env python3
"""Independent frozen matrix of whole-entry context ordering; no model calls."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.config import NeoMemoryConfig  # noqa: E402
from core.context_bundle import assemble_context  # noqa: E402

BUDGETS = (512, 1024, 2048, 4096)
ORDERS = ("short_term_first", "profile_first", "relationship_first")
PRIORITIES = ("recall_first", "context_first")
TEXTS = {
    "short_term": "地图尚未校对，明天确认后再送印。",
    "profile": "喜欢观察云层，只有能见度合适时才参加户外活动。",
    "relationship": "我愿意耐心交流，也希望彼此能明确表达不同意见。",
}


def fixtures():
    cases = []
    for lengths in itertools.product((1, 8, 32), repeat=3):
        texts = {key: text * n for (key, text), n in zip(TEXTS.items(), lengths)}
        cases.append({"name": "lengths_" + "_".join(map(str, lengths)), "snapshot": {
            "short_term": {"summary_text": texts["short_term"]},
            "profile": {"tags": ["户外观察"], "description": texts["profile"]},
            "relationship": {"state_text": texts["relationship"]}}})
    for only in (*TEXTS, "none"):
        snapshot = next(iter(cases))["snapshot"].copy()
        cases.append({"name": "only_" + only, "snapshot": {k: v for k, v in snapshot.items() if k == only}})
    return cases


def evaluate():
    cases = fixtures()
    hits = [{"node_id": "map", "node_type": "event", "status": "active", "content_text":
             "我和{{user:independent}}一起检查地图，路线尚未正式开放。" * 4},
            {"node_id": "weather", "node_type": "atomic", "status": "active", "content_text":
             "观测因天气推迟，尚未确定新的日期。"}]
    definition = {"version": "context-order-independent.v1", "cases": cases, "hits": hits,
                  "budgets": BUDGETS, "orders": ORDERS, "priorities": PRIORITIES}
    results = []
    for case, budget, priority, order in itertools.product(cases, BUDGETS, PRIORITIES, ORDERS):
        config = NeoMemoryConfig.from_mapping({"recall_budget_tokens": budget,
            "context_budget_priority": priority, "context_entry_order": order})
        bundle = assemble_context(case["snapshot"], hits, config, user_names={"independent": "访客🌤"})
        selected = []
        whole = True
        for section, value in case["snapshot"].items():
            text = value.get("summary_text", value.get("description", value.get("state_text")))
            omitted = any(x.startswith(section + ":") for x in bundle.diagnostics["context_omitted"])
            if not omitted:
                selected.append(section)
                whole = whole and text in bundle.text
            else:
                whole = whole and text not in bundle.text
        results.append({"case": case["name"], "budget": budget, "priority": priority, "order": order,
                        "selected_sections": selected, "selected_nodes": [x["node_id"] for x in bundle.hits],
                        "whole": whole, "budget_ok": len(bundle.text.encode()) <= budget,
                        "diagnostics": bundle.diagnostics})
    return {"fixture_sha256": hashlib.sha256(json.dumps(definition, ensure_ascii=False, sort_keys=True).encode()).hexdigest(),
            "implementation_sha256": hashlib.sha256((ROOT / "core/context_bundle.py").read_bytes()).hexdigest(),
            "cases": len(results), "failures": sum(not x["whole"] or not x["budget_ok"] for x in results), "results": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = evaluate()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "results"}))
    return bool(report["failures"])


if __name__ == "__main__":
    raise SystemExit(main())
