"""Offline residual-budget coverage top-up; preserve every baseline entry."""
from __future__ import annotations

import argparse
from dataclasses import replace
import hashlib
import json
from pathlib import Path

from core.config import NeoMemoryConfig
from core.context_bundle import ContextBundle, assemble_context
from core.retrieval import MEMORY_PREFIX, RecallPolicy, render_nodes, select_context
from scripts.coverage_policy_probe import synthetic_cases
from scripts.probe_quality_coverage_consumption import save


def top_up(bundle, ranked, coverage, config, *, user_names=None):
    """Append only current direct atoms suppressed by an actually selected full parent."""
    selected = list(bundle.hits)
    by_id = {node["node_id"]: node for node in ranked}
    selected_ids = {node["node_id"] for node in selected}
    suppressed = {row["atomic_node_id"] for row in coverage
        if row["coverage_kind"] == "full" and row["event_node_id"] in selected_ids
        and row["atomic_node_id"] in by_id and row["event_node_id"] in by_id
        and row["event_content_version"] == by_id[row["event_node_id"]]["content_version"]
        and row["atomic_content_version"] == by_id[row["atomic_node_id"]]["content_version"]}
    prefix = bundle.text.split(MEMORY_PREFIX, 1)[0]
    text = bundle.text
    added, skipped = [], []
    for node in ranked:
        if (node["node_id"] not in suppressed or node["node_id"] in selected_ids
                or not node.get("direct") or not node.get("current")):
            continue
        proposed = selected + [node]
        proposed_text = prefix + render_nodes(proposed, user_names)
        reason = "node_limit" if len(proposed) > config.recall_max_nodes else (
            "budget" if len(proposed_text.encode()) > config.recall_budget_tokens else None)
        if reason:
            skipped.append({"node_id": node["node_id"], "reason": reason})
            continue
        selected = proposed; text = proposed_text
        selected_ids.add(node["node_id"]); added.append(node["node_id"])
    return {"text": text, "hits": selected, "added_ids": added, "skipped": skipped,
            "injected_bytes": len(text.encode()), "extra_bytes": len(text.encode()) - len(bundle.text.encode())}


def run(source, directory):
    contexts_path = source / "contexts.json"
    contexts = [row for row in json.loads(contexts_path.read_text()) if row["policy"] == "declared_full"]
    directory.mkdir(parents=True, exist_ok=False)
    spec = {"source_context_sha256": hashlib.sha256(contexts_path.read_bytes()).hexdigest(),
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "policy": "Preserve baseline text and selected entries, append covered current direct atoms only while remaining byte and node budgets allow.",
            "answer_calls": 0, "embedding_calls": 0, "posthoc_reuse_of_observed_inputs": True}
    save(directory / "manifest.json", spec)
    results = []
    for original in contexts:
        payload = json.loads((source / original["source_model"] / f"recall-{original['query']}.json").read_text())
        config = NeoMemoryConfig(recall_budget_tokens=original["budget"])
        names = {"synthetic_coverage_visitor": "访客"}
        bundle = assemble_context({}, payload["candidates"], config, user_names=names, coverage=payload["coverage"])
        assert bundle.text == original["text"]
        improved = top_up(bundle, payload["candidates"], payload["coverage"], config, user_names=names)
        assert [node["node_id"] for node in improved["hits"][:len(bundle.hits)]] == [node["node_id"] for node in bundle.hits]
        assert improved["injected_bytes"] <= original["budget"]
        results.append({**{key: original[key] for key in ("source_model", "query", "budget")},
            "baseline_bytes": original["diagnostics"]["injected_bytes"], "baseline_ids": original["hit_ids"], **improved})
    save(directory / "results.json", results)
    mechanics = []
    for case in synthetic_cases():
        for origin in ("current_direct", "neighbor"):
            ranked = [{**item, "direct": origin == "current_direct" or item["node_id"] != "a",
                       "current": origin == "current_direct" or item["node_id"] != "a"} for item in case["ranked"]]
            if origin == "neighbor":
                ranked = [node for node in ranked if node["node_id"] != "a"] + [node for node in ranked if node["node_id"] == "a"]
            for budget in (2048, 4096):
                for maximum in (2, 6):
                    baseline = select_context(ranked, case["coverage"], replace(RecallPolicy(), budget_tokens=budget, max_nodes=maximum))
                    bundle = ContextBundle(render_nodes(baseline), baseline, {})
                    improved = top_up(bundle, ranked, case["coverage"], NeoMemoryConfig(recall_budget_tokens=budget, recall_max_nodes=maximum))
                    before = {node["node_id"] for node in baseline}
                    after = {node["node_id"] for node in improved["hits"]}
                    assert before <= after and improved["injected_bytes"] <= budget and len(after) <= maximum
                    assert improved["extra_bytes"] >= 0
                    if origin == "neighbor":
                        assert before == after
                    mechanics.append({"case": case["id"], "origin": origin, "budget": budget, "max_nodes": maximum,
                        "baseline_ids": sorted(before), "topup_ids": sorted(after), "added_ids": improved["added_ids"],
                        "baseline_facts": sum(bool(before.intersection(nodes)) for nodes in case["facts"].values()),
                        "topup_facts": sum(bool(after.intersection(nodes)) for nodes in case["facts"].values()),
                        "extra_bytes": improved["extra_bytes"]})
    save(directory / "mechanics.json", mechanics)
    return {"real_recall_contexts": len(results), "contexts_with_additions": sum(bool(row["added_ids"]) for row in results),
            "mechanism_checks": len(mechanics), "baseline_preserved": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.source, args.directory)))


if __name__ == "__main__":
    main()
