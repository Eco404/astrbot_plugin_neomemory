"""Offline boundary checks for direct-hit coverage protection; no semantic model."""
from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import replace
import hashlib
import json
from pathlib import Path

from core.retrieval import RecallPolicy, render_nodes, select_context
from scripts.coverage_policy_probe import synthetic_cases
from scripts.probe_quality_coverage_consumption import POLICIES, filtered_coverage, save


def run(directory):
    cases = synthetic_cases()
    spec = {"cases": cases, "policies": POLICIES, "budgets": [2048, 4096], "max_nodes": [2, 6],
            "atomic_origins": ["current_direct", "neighbor"], "synthetic_only": True,
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "limitation": "手写rank只验证机制与人工标注事实节点可达性，不代表实际语义检索或回答质量。邻居分支把原子移到直接候选之后。"}
    directory.mkdir(parents=True, exist_ok=False)
    save(directory / "manifest.json", spec)
    results = []
    aggregate = defaultdict(lambda: {"facts_present": 0, "facts_total": 0, "duplicated_fact_support": 0, "bytes": 0})
    for case in cases:
        for origin in spec["atomic_origins"]:
            ranked = [{**item, "direct": origin == "current_direct" or item["node_id"] != "a",
                       "current": origin == "current_direct" or item["node_id"] != "a"} for item in case["ranked"]]
            if origin == "neighbor":
                ranked = [item for item in ranked if item["node_id"] != "a"] + [item for item in ranked if item["node_id"] == "a"]
            for budget in spec["budgets"]:
                for maximum in spec["max_nodes"]:
                    for policy in spec["policies"]:
                        selection = select_context(ranked, filtered_coverage(ranked, case["coverage"], policy),
                            replace(RecallPolicy(), budget_tokens=budget, max_nodes=maximum))
                        ids = {item["node_id"] for item in selection}
                        supports = {fact: len(ids.intersection(nodes)) for fact, nodes in case["facts"].items()}
                        result = {"case": case["id"], "origin": origin, "budget": budget, "max_nodes": maximum,
                                  "policy": policy, "hit_ids": sorted(ids), "fact_support_counts": supports,
                                  "bytes": len(render_nodes(selection).encode())}
                        assert result["bytes"] <= budget and len(selection) <= maximum
                        results.append(result)
                        summary = aggregate[(origin, budget, maximum, policy)]
                        summary["facts_present"] += sum(value > 0 for value in supports.values())
                        summary["facts_total"] += len(supports)
                        summary["duplicated_fact_support"] += sum(max(0, value - 1) for value in supports.values())
                        summary["bytes"] += result["bytes"]
    summaries = [{"origin": key[0], "budget": key[1], "max_nodes": key[2], "policy": key[3], **value}
                 for key, value in aggregate.items()]
    save(directory / "results.json", results); save(directory / "summary.json", summaries)
    # Attached atoms preserve baseline full behavior in this explicit mechanism check.
    by_key = {(row["case"], row["origin"], row["budget"], row["max_nodes"], row["policy"]): row for row in results}
    for row in results:
        if row["origin"] == "neighbor":
            baseline = by_key[(row["case"], row["origin"], row["budget"], row["max_nodes"], "declared_full")]
            assert row["hit_ids"] == baseline["hit_ids"]
    return {"checks": len(results), "budget_and_node_limits": True, "neighbor_semantics_unchanged": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.directory)))


if __name__ == "__main__":
    main()
