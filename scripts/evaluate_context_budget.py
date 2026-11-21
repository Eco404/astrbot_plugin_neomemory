#!/usr/bin/env python3
"""Evaluate deterministic context packing with frozen, independent synthetic input.

This measures selection, complete-entry preservation and local assembly time.
It does not measure retrieval relevance, model quality or real chat behavior.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Direct script execution needs the repository root before project imports.
from core.context_bundle import assemble_context  # noqa: E402
from core.memory_text import render_memory_text  # noqa: E402
from core.retrieval import render_nodes  # noqa: E402

FIXTURE_VERSION = "context-budget-synthetic.v1"
BUDGETS = (256, 512, 1024, 2048, 4096)
PRIORITIES = ("recall_first", "context_first")
LABELS = {
    "notes": "活跃笔记提示",
    "short_term": "较早会话摘要（较新消息优先；其中待办可能已在后续完成）",
    "profile": "用户画像（当前发言人的资料）",
    "relationship": "人格关系（第一人称指当前人格；主观感受不代表对方的客观态度）",
}
PROSE = {
    "zh": "我记得{{user:lu_synthetic}}在周末学习陶艺，只有提前预约时才去工作室。",
    "en": "I remember {{user:lu_synthetic}} practices pottery on weekends, only with an advance booking. ",
    "ja": "私は{{user:lu_synthetic}}が事前に予約した週末だけ陶芸を学ぶと覚えている。",
    "ar": "أتذكر أن {{user:lu_synthetic}} يتعلم صناعة الفخار في عطلة الأسبوع فقط بعد الحجز. ",
    "hi": "मुझे याद है कि {{user:lu_synthetic}} अग्रिम बुकिंग होने पर ही सप्ताहांत में मिट्टी के बर्तन बनाना सीखता है। ",
    "emoji": "我记得{{user:lu_synthetic}}画过🌿🧑🏽‍🎨🏳️‍🌈，约定下次一起看作品；不是已经参观。",
}


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()


def node(node_id, content, *, current=True):
    return {"node_id": node_id, "node_type": "event", "status": "active",
            "content_text": content, "event_time_text": "一个约定的周末",
            "current": current, "direct": True}


def scenario(name, *, language="zh", repeats=1, hits=True, context=True, names=None,
             overrides=None, first_oversized=False, oversized_profile=False):
    text = PROSE[language] * repeats
    candidates = ([node("synthetic_primary", text),
                   node("synthetic_secondary", "另一段完整回忆：工具课尚未预约。", current=False)] if hits else [])
    if first_oversized:
        candidates.insert(0, node("synthetic_oversized", PROSE["en"] * 100))
    snapshot = {}
    if context:
        snapshot = {
            "short_term": {"summary_text": "我们正在讨论工作室开放时间；预约尚未确认。"},
            "profile": {"tags": ["陶艺爱好者", "需要事先预约"],
                        "description": "{{user:lu_synthetic}}喜欢尝试新材料，但需要保留完整的预约条件。"},
            "relationship": {"state_text": "我期待听{{user:lu_synthetic}}谈论新作品，也愿意等对方方便时再聊。"},
            "active_notes": 2,
        }
        if oversized_profile:
            snapshot["profile"]["description"] = "独立合成的超长画像，保持整个条目。" * 150
    return {"name": name, "snapshot": snapshot, "hits": candidates,
            "names": names if names is not None else {"lu_synthetic": "合成访客"},
            "overrides": overrides or {}}


def scenarios():
    return [
        *(scenario("language_" + language, language=language, repeats=3) for language in PROSE),
        scenario("compact_entries"),
        scenario("long_primary", repeats=15),
        scenario("oversized_first", first_oversized=True),
        scenario("oversized_profile", oversized_profile=True),
        scenario("no_long_term", hits=False),
        scenario("long_term_only", context=False),
        scenario("long_nickname", names={"lu_synthetic": "合成昵称🌿" * 24}),
        scenario("missing_nickname", names={}),
        scenario("disabled_sections", overrides={"short_term_injection_enabled": False,
                 "profile_injection_enabled": False, "relationship_injection_enabled": False,
                 "notes_hint_enabled": False}),
        scenario("tools_disabled", overrides={"tools_enabled": False}),
        scenario("empty", hits=False, context=False),
    ]


def expected_payloads(case):
    snapshot, names = case["snapshot"], case["names"]
    result = {}
    if snapshot.get("active_notes"):
        result["notes"] = {"count": snapshot["active_notes"], "tool": "neomemory_notes", "action": "list"}
    if snapshot.get("short_term"):
        result["short_term"] = snapshot["short_term"]["summary_text"]
    if snapshot.get("profile"):
        profile = snapshot["profile"]
        result["profile"] = {"tags": [render_memory_text(tag, names) for tag in profile["tags"]],
                             "description": render_memory_text(profile["description"], names)}
    if snapshot.get("relationship"):
        result["relationship"] = render_memory_text(snapshot["relationship"]["state_text"], names)
    return result


def inspect_bundle(case, bundle, config):
    selected_context = []
    expected = expected_payloads(case)
    whole_context = True
    for name, label in LABELS.items():
        lines = [line[len(label) + 1:] for line in bundle.text.splitlines() if line.startswith(label + "：")]
        if lines:
            selected_context.append(name)
            whole_context = whole_context and len(lines) == 1 and json.loads(lines[0]) == expected.get(name)
    hit_ids = [hit["node_id"] for hit in bundle.hits]
    known = {hit["node_id"]: hit for hit in case["hits"]}
    whole_hits = all(hit == known.get(hit["node_id"]) for hit in bundle.hits)
    whole_hits = whole_hits and (not bundle.hits or render_nodes(bundle.hits, case["names"]) in bundle.text)
    first_fitting = next((hit["node_id"] for hit in case["hits"]
                          if len(render_nodes([hit], case["names"]).encode()) <= config.recall_budget_tokens), None)
    flags = {"short_term": config.short_term_injection_enabled,
             "profile": config.profile_injection_enabled,
             "relationship": config.relationship_injection_enabled,
             "notes": config.notes_hint_enabled and config.tools_enabled}
    return {"selected_node_ids": hit_ids, "selected_context": selected_context,
            "primary_selected": bool(case["hits"] and case["hits"][0]["node_id"] in hit_ids),
            "first_fitting_node_id": first_fitting,
            "first_fitting_selected": first_fitting in hit_ids if first_fitting else None,
            "whole_content_ok": bool(whole_context and whole_hits),
            "disabled_sections_ok": all(flags[name] for name in selected_context),
            "budget_ok": len(bundle.text.encode()) <= config.recall_budget_tokens,
            "injected_bytes": len(bundle.text.encode()), "diagnostics": bundle.diagnostics,
            "rendered_text": bundle.text}


def evaluate(iterations):
    cases = scenarios()
    records = []
    defaults = {"tools_enabled": True, "notes_hint_enabled": True,
                "short_term_injection_enabled": True, "profile_injection_enabled": True,
                "relationship_injection_enabled": True, "context_short_term_max_chars": 1200,
                "context_profile_max_chars": 1200, "context_relationship_max_chars": 1200}
    fixture_hash = digest({"version": FIXTURE_VERSION, "cases": cases, "budgets": BUDGETS,
                           "priorities": PRIORITIES, "defaults": defaults})
    for case in cases:
        for budget in BUDGETS:
            for priority in PRIORITIES:
                cfg = SimpleNamespace(**{**defaults, **case["overrides"],
                    "recall_budget_tokens": budget, "context_budget_priority": priority})
                durations = []
                for _ in range(iterations):
                    start = time.perf_counter_ns()
                    bundle = assemble_context(case["snapshot"], case["hits"], cfg, user_names=case["names"])
                    durations.append((time.perf_counter_ns() - start) / 1000)
                records.append({"scenario": case["name"], "budget_bytes": budget, "priority": priority,
                    **inspect_bundle(case, bundle, cfg), "assembly_median_us": statistics.median(durations),
                    "assembly_max_us": max(durations)})
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    return {"fixture_version": FIXTURE_VERSION, "fixture_hash": fixture_hash,
            "scope": "Synthetic deterministic packing only; no semantic quality or real-chat claim.",
            "git_head": revision, "iterations_per_case": iterations,
            "implementation_sha256": {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
                for path in ("core/context_bundle.py", "core/retrieval.py", "core/memory_text.py")},
            "summary": {"cases": len(records),
                "whole_content_failures": sum(not r["whole_content_ok"] for r in records),
                "budget_failures": sum(not r["budget_ok"] for r in records),
                "disabled_section_failures": sum(not r["disabled_sections_ok"] for r in records)},
            "results": records}


def compare(report, baseline):
    if baseline["fixture_hash"] != report["fixture_hash"]:
        raise ValueError("baseline fixtures differ; comparisons require the same frozen input")
    previous = {(r["scenario"], r["budget_bytes"], r["priority"]): r for r in baseline["results"]}
    changes = []
    for row in report["results"]:
        old = previous[(row["scenario"], row["budget_bytes"], row["priority"])]
        changes.append({"scenario": row["scenario"], "budget_bytes": row["budget_bytes"],
            "priority": row["priority"], "nodes_added": sorted(set(row["selected_node_ids"]) - set(old["selected_node_ids"])),
            "nodes_removed": sorted(set(old["selected_node_ids"]) - set(row["selected_node_ids"])),
            "context_added": sorted(set(row["selected_context"]) - set(old["selected_context"])),
            "context_removed": sorted(set(old["selected_context"]) - set(row["selected_context"])),
            "injected_bytes_delta": row["injected_bytes"] - old["injected_bytes"],
            "assembly_median_us_delta": row["assembly_median_us"] - old["assembly_median_us"]})
    report["comparison"] = {"baseline_implementation_sha256": baseline["implementation_sha256"], "changes": changes}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--iterations", type=int, default=25)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    report = evaluate(args.iterations)
    if args.baseline:
        compare(report, json.loads(args.baseline.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "fixture_hash": report["fixture_hash"],
                      **report["summary"]}, ensure_ascii=False))
    return int(any(report["summary"][name] for name in
                   ("whole_content_failures", "budget_failures", "disabled_section_failures")))


if __name__ == "__main__":
    raise SystemExit(main())
