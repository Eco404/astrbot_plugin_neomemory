"""Offline comparison of coverage filters around the real context selector.

The fact labels below are handwritten fixture annotations, not a semantic judge.
No provider, embedding, production configuration or database is used or changed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

from core.retrieval import RecallPolicy, render_nodes, select_context

POLICIES = ("declared_full", "keep_both", "literal_contains")
BUDGETS = {
    "default": RecallPolicy(),
    "byte_limited": RecallPolicy(budget_tokens=768),
    "node_limited": RecallPolicy(max_nodes=2),
}


def _node(node_id: str, kind: str, content: str, version: int = 1) -> dict[str, Any]:
    return {"node_id": node_id, "node_type": kind, "content_text": content,
            "content_version": version, "status": "active", "connection": "直接匹配"}


def _case(case_id, atomic, event, facts, *, coverage_kind="full", stale=False,
          event_first=False, other=None, description=""):
    atom = _node("a", "atomic", atomic)
    parent = _node("e", "event", event)
    nodes = [parent, atom] if event_first else [atom, parent]
    if other is not None:
        nodes.append(_node("b", "atomic", other))
    return {"id": case_id, "description": description, "ranked": nodes,
            "coverage": [{"event_node_id": "e", "atomic_node_id": "a",
                          "event_content_version": 0 if stale else 1,
                          "atomic_content_version": 1, "coverage_kind": coverage_kind}],
            "facts": facts, "provenance": "original_independent_synthetic_fixture"}


def synthetic_cases() -> list[dict[str, Any]]:
    """Freeze the same fact obligations for every selector and budget."""
    atomic = "夜间观测仅限预约者参加。"
    return [
        _case("literal_full", atomic, atomic + "本次观测地点为北侧平台。",
              {"reservation_required": ["a", "e"], "north_platform": ["e"],
               "independent_preference": ["b"]}, other="用户偏好用纸质星图。",
              description="正确full；两个节点加独立原子可暴露节点预算挤占。"),
        _case("paraphrased_full", atomic, "未预约者不能参加夜间观测；本次地点为北侧平台。",
              {"reservation_required": ["a", "e"], "north_platform": ["e"],
               "independent_preference": ["b"]}, other="用户偏好用纸质星图。",
              description="事件语义完整，逐字包含规则会漏掉合理省略机会。"),
        _case("necessary_condition", "工作坊必须有安全员在场才能开始。",
              "工作坊已安排在周日下午，安全员确认当天下午有空。",
              {"safety_officer_required": ["a"], "sunday_afternoon": ["e"]}, event_first=True,
              description="有空不等于必须在场；错误full不能仅凭版本匹配获信任。"),
        _case("quoted_unverified", "双语讲座只在周六举行。",
              "留言中出现了“双语讲座只在周六举行。”，组织者尚未核实这条留言。",
              {"saturday_only_confirmed": ["a"], "unverified_message": ["e"]},
              description="原子为已确认陈述；事件只转述未核实留言，逐字包含不保证断言立场。"),
        _case("historical_quote", "用户每周五值班。",
              "五年前的一份记录写着“用户每周五值班。”，本月只记录了一次临时调班。",
              {"current_friday_duty": ["a"], "one_temporary_swap_this_month": ["e"]},
              description="原子的当前安排与事件中的五年前引用不同；逐字包含不保留时间范围。"),
        _case("declared_partial", "归还借阅设备前必须清空个人文件。", "用户周一归还了借阅设备。",
              {"clear_files_required": ["a"], "return_on_monday": ["e"]}, coverage_kind="partial",
              description="partial始终不能触发覆盖省略。"),
        _case("stale_full", "今后周三不参加排练。", "之前用户固定周三参加排练。",
              {"current_no_wednesday": ["a"], "prior_wednesday": ["e"]}, stale=True,
              description="绑定旧事件版本的full无效，三种策略都保留可容纳的两端。"),
        _case("oversized_parent", atomic, atomic + "背景资料记载天气与器材准备。" * 70,
              {"reservation_required": ["a", "e"]},
              description="父事件自身超默认预算；不能因覆盖记录而省掉可容纳原子。"),
        _case("byte_competition", atomic,
              "未预约者不能参加夜间观测；本次地点为北侧平台。" + "背景资料记载天气与器材准备。" * 5,
              {"reservation_required": ["a", "e"], "north_platform": ["e"]},
              description="原子先入选后，保留两端可能容纳不了事件，丢失事件独有信息。"),
        _case("false_full_budget_pressure", "工作坊必须有安全员在场才能开始。",
              "工作坊已安排在周日下午，安全员确认当天下午有空。" + "背景资料记载工具与场地准备。" * 5,
              {"safety_officer_required": ["a"], "sunday_afternoon": ["e"]}, event_first=True,
              description="事件先占满预算时，即使不做覆盖省略也不保证原子最终能进入上下文。"),
    ]


def previous_synthetic_case(path: Path) -> dict[str, Any]:
    """Read only the named, already-reviewed synthetic regression artifact.

    This entry does not evaluate other claims in those nodes. It checks the one
    manually reviewed necessary condition from the preceding fidelity round.
    """
    document = json.loads(path.read_text())
    after = document["after"]
    expected = {
        "node_coverage_partial_seed_seed_partial_atomic": "atomic",
        "node_coverage_partial_seed_seed_partial_event": "event",
    }
    nodes = {node["node_id"]: node for node in after["nodes"]}
    if set(nodes) != set(expected) or any(nodes[key]["node_type"] != kind for key, kind in expected.items()):
        raise ValueError("expected the named two-node synthetic fidelity regression")
    atomic_id, event_id = expected
    if ("须有带队老师全程在场" not in nodes[atomic_id]["content_text"]
            or "须有带队老师全程在场" in nodes[event_id]["content_text"]):
        raise ValueError("synthetic regression no longer matches its reviewed condition")
    return {"id": "observed_synthetic_v5_condition", "description": "上轮独立合成已审阅错误full的原始端点。",
            "ranked": [nodes[event_id], nodes[atomic_id]], "coverage": after["coverage"],
            "facts": {"teacher_presence_is_required": [atomic_id]},
            "provenance": {"kind": "previous_synthetic_evaluation",
                           "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}}


def filter_coverage(ranked: list[dict], coverage: list[dict], mode: str) -> list[dict]:
    if mode not in POLICIES:
        raise ValueError(f"unknown experimental policy: {mode}")
    if mode == "keep_both":
        return []
    if mode == "declared_full":
        return coverage
    by_id = {item["node_id"]: item for item in ranked}
    return [row for row in coverage
            if row["event_node_id"] in by_id and row["atomic_node_id"] in by_id
            and by_id[row["atomic_node_id"]]["content_text"]
            and by_id[row["atomic_node_id"]]["content_text"] in by_id[row["event_node_id"]]["content_text"]]


def evaluate(case: dict, mode: str, budget: RecallPolicy) -> dict:
    ranked = case["ranked"]
    selected = select_context(ranked, filter_coverage(ranked, case["coverage"], mode), budget)
    # A larger budget separates coverage removal from capacity. No vector or
    # relevance stage is being tested: the candidate order is fixed input.
    unlimited = select_context(ranked, filter_coverage(ranked, case["coverage"], mode),
                               RecallPolicy(max_nodes=30, budget_tokens=32000))
    ids = {node["node_id"] for node in selected}
    occurrences = {fact: len(ids.intersection(support)) for fact, support in case["facts"].items()}
    by_id = {item["node_id"]: item for item in ranked}
    duplicate_pairs = []
    for row in case["coverage"]:
        if row["event_node_id"] in ids and row["atomic_node_id"] in ids:
            event, atomic = by_id[row["event_node_id"]], by_id[row["atomic_node_id"]]
            if atomic["content_text"] and atomic["content_text"] in event["content_text"]:
                duplicate_pairs.append({"event_id": event["node_id"], "atomic_id": atomic["node_id"],
                                        "bytes": len(atomic["content_text"].encode("utf-8"))})
    text = render_nodes(selected)
    return {"selected_ids": [node["node_id"] for node in selected],
            "missing_facts": [fact for fact, count in occurrences.items() if count == 0],
            "fact_occurrences": occurrences,
            "retained_fact_count": sum(count > 0 for count in occurrences.values()),
            "required_fact_count": len(occurrences),
            "duplicate_fact_occurrences": sum(max(0, count - 1) for count in occurrences.values()),
            "literal_overlap_pairs": duplicate_pairs,
            "literal_overlap_pair_bytes": sum(pair["bytes"] for pair in duplicate_pairs),
            "rendered_bytes": len(text.encode("utf-8")), "text": text,
            "capacity_excluded_ids": [node["node_id"] for node in unlimited if node["node_id"] not in ids],
            "without_capacity_limits_ids": [node["node_id"] for node in unlimited],
            "coverage_removed_without_capacity_ids": [node["node_id"] for node in ranked
                if node["node_id"] not in {item["node_id"] for item in unlimited}]}


def run_probe(cases: list[dict]) -> dict:
    rows = []
    for case in cases:
        for budget_name, budget in BUDGETS.items():
            results = {mode: evaluate(case, mode, budget) for mode in POLICIES}
            baseline = results["declared_full"]["rendered_bytes"]
            for mode, result in results.items():
                result["bytes_delta_vs_declared_full"] = result["rendered_bytes"] - baseline
                rows.append({"case_id": case["id"], "budget": budget_name, "policy": mode, **result})
    summary = {}
    for budget_name in BUDGETS:
        for mode in POLICIES:
            group = [row for row in rows if row["budget"] == budget_name and row["policy"] == mode]
            totals = Counter()
            for row in group:
                for key in ("retained_fact_count", "required_fact_count", "duplicate_fact_occurrences",
                            "literal_overlap_pair_bytes", "rendered_bytes", "bytes_delta_vs_declared_full"):
                    totals[key] += row[key]
            summary[f"{budget_name}/{mode}"] = {"case_count": len(group),
                "all_annotated_facts_present_cases": sum(not row["missing_facts"] for row in group), **totals}
    return {"schema_version": "coverage-policy-probe.v1", "llm_calls": 0, "embedding_calls": 0,
            "metric_warning": "Handwritten node-level fact obligations; not a semantic judge or corpus quality rate. "
                              "Bytes use the production renderer; not measured model tokens. Literal overlap is not equivalence.",
            "summary": summary, "results": rows}


def _write_new(path: Path, data: dict) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--previous-synthetic-evidence", type=Path)
    args = parser.parse_args(argv)
    cases = synthetic_cases()
    if args.previous_synthetic_evidence:
        cases.append(previous_synthetic_case(args.previous_synthetic_evidence))
    args.directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    source_paths = (Path(__file__), Path(__file__).parents[1] / "core" / "retrieval.py")
    manifest = {"schema_version": "coverage-policy-manifest.v1", "cases": cases,
                "policies": POLICIES, "budgets": {name: asdict(value) for name, value in BUDGETS.items()},
                "source_sha256": {str(path.relative_to(Path(__file__).parents[1])):
                                  hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths}}
    # Write case obligations before collecting any candidate result. The single
    # run creates an immutable directory and never overwrites an earlier probe.
    _write_new(args.directory / "manifest.json", manifest)
    report = run_probe(cases)
    report["manifest_sha256"] = hashlib.sha256((args.directory / "manifest.json").read_bytes()).hexdigest()
    _write_new(args.directory / "report.json", report)
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
