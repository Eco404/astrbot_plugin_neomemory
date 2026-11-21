"""Frozen coverage consumption experiment with real vector recall and answers.

Imports unchanged generated prose from independent synthetic case artifacts into
an explicitly composite test pool. Only identity references and version bindings
are remapped for import; no extraction, semantic repair or production write.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace

from scripts.verify_stage4_api import credentials, failure

POLICIES = ("declared_full", "protect_current_direct", "protect_leading_direct")
BUDGETS = (2048, 4096)
QUERIES = (
    {"text": "我平时参加观星活动有哪些限制？如果朋友想借双筒镜，我此前是怎么说的？",
     "needed": "喜欢夜间观星，只参加领队全程在场的开放活动，不参加收费营，个人双筒镜不外借。一次没有借出不能代替稳定不外借规则。",
     "relevant_cases": ["coverage"]},
    {"text": "最近那次观星怎么样？另外，蓝釉碗和绿釉盘分别到哪一步了？",
     "needed": "观星已结束，领队全程在场，用自己的双筒镜看木星；蓝碗烧好并取走；绿盘刻了姓，已入窑尚未出窑。",
     "relevant_cases": ["coverage", "identity"]},
)


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def filtered_coverage(ranked, coverage, policy):
    """Keep full semantics unchanged except protection of selected direct hits."""
    if policy == "declared_full":
        return list(coverage)
    positions = {item["node_id"]: index for index, item in enumerate(ranked)}
    by_id = {item["node_id"]: item for item in ranked}
    result = []
    for row in coverage:
        atom = by_id.get(row["atomic_node_id"])
        protected = bool(atom and atom.get("direct") and atom.get("current"))
        if policy == "protect_leading_direct":
            protected = protected and positions[atom["node_id"]] < positions.get(row["event_node_id"], len(ranked))
        if not protected:
            result.append(row)
    return result


def freeze(source, directory):
    original = json.loads((source / "manifest.json").read_text())["specification"]
    assert original["synthetic_only"] is True
    pools = []
    for model in original["models"]:
        artifacts = []
        for case in original["cases"]:
            arm = source / model / case["name"] / "candidate"
            snapshots = sorted(arm.glob("window-*/after.json"), key=lambda p: int(p.parent.name.split("-")[-1]))
            if not snapshots:
                artifacts.append({"case": case["name"], "last_committed_window": 0, "nodes": [], "coverage": [], "edges": []})
                continue
            last = snapshots[-1]; data = json.loads(last.read_text())
            nodes = {node["node_id"]: node for node in data["nodes"]}
            usable = [row for row in data["coverage"] if row["event_node_id"] in nodes and row["atomic_node_id"] in nodes
                and nodes[row["event_node_id"]]["content_version"] == row["event_content_version"]
                and nodes[row["atomic_node_id"]]["content_version"] == row["atomic_content_version"]]
            db = sqlite3.connect((arm / "memory.db").resolve().as_uri() + "?mode=ro", uri=True)
            db.row_factory = sqlite3.Row
            edges = [dict(row) for row in db.execute("SELECT * FROM memory_edges")
                     if row["source_node_id"] in nodes and row["target_node_id"] in nodes]
            db.close()
            artifacts.append({"case": case["name"], "last_committed_window": int(last.parent.name.split("-")[-1]),
                "artifact_sha256": hashlib.sha256(last.read_bytes()).hexdigest(), "source_path": str(last),
                "nodes": list(nodes.values()), "coverage": usable, "edges": edges})
        pools.append({"source_model": model, "artifacts": artifacts})
    root = Path(__file__).resolve().parents[1]
    spec = {"pools": pools, "queries": QUERIES, "policies": POLICIES, "budgets": BUDGETS,
        "answer_model": "gpt-6-luna", "answer_http_limit": 24, "embedding_http_limit": 16,
        "synthetic_only": True, "already_seen_generated_prose": True, "composite_pool": True,
        "candidate_arm_selection": "all last successfully committed candidate-arm outputs, including errors and duplication",
        "identity_map": "all synthetic user references map to one test visitor; imported nodes start version 1 and bind coverage to those immutable imported texts",
        "production_default_changed": False, "transport_retries": 0, "answer_retries": 0,
        "review": "逐题审阅必要事实、确定性和主体、回答无依据补全、无关信息和字节代价。保护直接命中不证明原子自身正确。对照共享真实召回候选与同一预算，只改变coverage消费；不改排序、正文、强度和阈值。人工合成竞争池不是自然生成整链或生产错误率。",
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "retrieval_sha256": hashlib.sha256((root / "core/retrieval.py").read_bytes()).hexdigest(),
        "context_sha256": hashlib.sha256((root / "core/context_bundle.py").read_bytes()).hexdigest()}
    directory.mkdir(parents=True, exist_ok=False)
    save(directory / "manifest.json", {"specification": spec, "sha256": digest(spec)})


async def prepare_pool(directory, spec, pool, adapter):
    from core.config import NeoMemoryConfig
    from core.context_bundle import assemble_context
    from core.embeddings import EmbeddingService
    from core.memory_spaces import private_space
    from core.memory_text import map_user_references
    from core.retrieval import RecallService
    from storage.cache_store import CacheStore
    from storage.memory_store import MemoryStore
    target = directory / pool["source_model"]; target.mkdir()
    memory = MemoryStore(target / "memory.db"); cache = CacheStore(target / "cache.db")
    await memory.open(); await cache.open()
    visitor = "synthetic_coverage_visitor"
    space = await memory.get_or_create_space(private_space("synthetic_coverage_persona", visitor))
    id_map, origin = {}, {}
    results = []
    try:
        for artifact in pool["artifacts"]:
            for node in artifact["nodes"]:
                def prose(text):
                    return map_user_references(text, lambda _old: visitor) if text else text
                emotion = node.get("emotion")
                if emotion:
                    emotion = {**emotion, "description": prose(emotion["description"])}
                new = await memory.create_node(space_id=space["space_id"], node_type=node["node_type"],
                    title=prose(node.get("title")), content_text=prose(node["content_text"]), emotion=emotion,
                    importance=node["importance"], strength=node["strength"],
                    event_time_start=node.get("event_time_start"), event_time_end=node.get("event_time_end"),
                    event_time_text=prose(node.get("event_time_text")), source_kind="external",
                    source_ids=["synthetic-artifact:" + artifact["artifact_sha256"] + ":" + node["node_id"]],
                    metadata={"synthetic_only": True, "origin_case": artifact["case"],
                              "origin_node_id": node["node_id"], "origin_version": node["content_version"]})
                id_map[node["node_id"]] = new["node_id"]
                origin[new["node_id"]] = artifact["case"]
        for artifact in pool["artifacts"]:
            for edge in artifact["edges"]:
                await memory.add_edge(id_map[edge["source_node_id"]], id_map[edge["target_node_id"]], edge["relation_type"])
            for row in artifact["coverage"]:
                await memory.add_coverage(id_map[row["event_node_id"]], id_map[row["atomic_node_id"]],
                    event_content_version=1, atomic_content_version=1, coverage_kind=row["coverage_kind"])
        vectors = EmbeddingService(adapter, cache, provider_id="coverage-embedding", batch_size=16)
        recall = RecallService(memory, vectors)
        await recall.index_space(space["space_id"], timeout_seconds=120)
        snapshot = await memory.retrieval_snapshot(space["space_id"])
        save(target / "import-map.json", {"id_map": id_map, "origin": origin, "snapshot": snapshot})
        for index, query in enumerate(spec["queries"], 1):
            retrieved = await recall.recall(space["space_id"], query["text"], user_names={visitor: "访客"}, retain_candidates=True)
            save(target / f"recall-{index}.json", {"query": query, "candidates": retrieved.candidates,
                "diagnostics": retrieved.diagnostics, "coverage": snapshot["coverage"]})
            for budget in spec["budgets"]:
                for policy in spec["policies"]:
                    coverage = filtered_coverage(retrieved.candidates, snapshot["coverage"], policy)
                    bundle = assemble_context({}, retrieved.candidates, NeoMemoryConfig(recall_budget_tokens=budget),
                        user_names=retrieved.user_names, coverage=coverage)
                    ids = {item["node_id"] for item in bundle.hits}
                    full_pairs = [row for row in snapshot["coverage"] if row["coverage_kind"] == "full"
                                  and row["event_node_id"] in ids and row["atomic_node_id"] in ids]
                    item = {"source_model": pool["source_model"], "query": index, "policy": policy, "budget": budget,
                        "query_text": query["text"], "needed_offline_only": query["needed"], "text": bundle.text,
                        "diagnostics": bundle.diagnostics, "hit_ids": list(ids),
                        "unrelated_origin_hits": [node_id for node_id in ids if origin[node_id] not in query["relevant_cases"]],
                        "declared_full_pairs_both_present": len(full_pairs),
                        "messages": [{"role": "system", "content": "你是知霁，依据提供的记忆自然回答访客。记忆未提供的信息不要编造，历史事件和长期习惯注意区分。\n" + bundle.text},
                                     {"role": "user", "content": query["text"]}]}
                    save(target / f"q{index}-{budget}-{policy}.json", item); results.append(item)
        return results
    finally:
        await memory.close(); await cache.close()


async def run(directory, secrets):
    from astrbot.core.provider.sources.openai_embedding_source import OpenAIEmbeddingProvider
    from astrbot.core.provider.sources.openai_responses_source import ProviderOpenAIResponses
    from astrbot.core.star.context import Context
    from adapters.providers import ProviderAdapter, _response_diagnostics
    manifest = json.loads((directory / "manifest.json").read_text()); spec = manifest["specification"]
    assert digest(spec) == manifest["sha256"]
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == spec["script_sha256"]
    with (directory / "started.json").open("x") as marker:
        json.dump({"started_at": time.time()}, marker)
    llm_cfg, emb_cfg = secrets["ecoapi"], secrets["Cloudflare Embedding"]
    embedding = OpenAIEmbeddingProvider({"id": "coverage-embedding", "type": "openai_embedding",
        "embedding_model": emb_cfg["model"], "embedding_api_base": emb_cfg["api_address"].rstrip("/") + "/accounts/" + emb_cfg["account_id"] + "/ai/v1",
        "embedding_api_key": emb_cfg["api_token"], "embedding_dimensions": 0, "embedding_dimensions_mode": "never", "timeout": 30}, {})
    embedding.client.max_retries = 0
    report = {"answer_http": 0, "embedding_http": 0, "answers": [], "semantic_review": "pending"}
    async def count_embedding(_request):
        if report["embedding_http"] >= spec["embedding_http_limit"]:
            raise RuntimeError("coverage_embedding_limit")
        report["embedding_http"] += 1
    embedding.client._client.event_hooks["request"].append(count_embedding)
    async def lookup(identifier):
        return embedding if identifier == "coverage-embedding" else None
    host = object.__new__(Context)
    host.provider_manager = SimpleNamespace(inst_map={"coverage-embedding": embedding}, provider_insts=[],
        embedding_provider_insts=[embedding], get_provider_by_id=lookup)
    adapter = ProviderAdapter(host, embedding_concurrency=1)
    started = time.monotonic()
    try:
        contexts = []
        for pool in spec["pools"]:
            contexts.extend(await prepare_pool(directory, spec, pool, adapter))
        save(directory / "contexts.json", contexts)
        semaphore = asyncio.Semaphore(2)
        async def answer(item):
            target = directory / item["source_model"] / f"answer-q{item['query']}-{item['budget']}-{item['policy']}"
            target.mkdir()
            provider = ProviderOpenAIResponses({"id": "coverage-answer", "type": "openai_responses", "model": spec["answer_model"],
                "api_base": llm_cfg["base_url"], "key": [llm_cfg["api_token"]], "timeout": 120}, {})
            provider.client.max_retries = 0
            sent = 0
            async def count(_request):
                nonlocal sent
                if sent or report["answer_http"] >= spec["answer_http_limit"]:
                    raise RuntimeError("coverage_answer_limit")
                sent += 1; report["answer_http"] += 1
            provider.client._client.event_hooks["request"].append(count)
            check = {key: item[key] for key in ("source_model", "query", "policy", "budget")}
            try:
                async with semaphore:
                    tick = time.monotonic()
                    try:
                        async with asyncio.timeout(125):
                            response = await provider.text_chat(contexts=item["messages"], request_max_retries=1)
                        (target / "answer.txt").write_text(response.completion_text, encoding="utf-8")
                        check.update(ok=True, diagnostics=_response_diagnostics(response))
                    except Exception as exc:
                        check.update(failure(exc))
                    check["seconds"] = time.monotonic() - tick
                    save(target / "check.json", check)
                    report["answers"].append(check); save(directory / "report.json", report)
            finally:
                await provider.terminate()
        await asyncio.gather(*(answer(item) for item in contexts))
        report["seconds"] = time.monotonic() - started
        save(directory / "report.json", report)
        return report
    finally:
        await embedding.terminate()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--freeze", action="store_true")
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    args = parser.parse_args()
    if args.freeze:
        freeze(args.source, args.directory); print(json.dumps({"frozen": True, "answer_http_limit": 24})); return
    (args.directory / "host/data/config").mkdir(parents=True, exist_ok=True)
    os.environ["ASTRBOT_ROOT"] = str((args.directory / "host").resolve())
    logging.disable(logging.CRITICAL)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from loguru import logger
        logger.remove(); result = asyncio.run(run(args.directory, credentials(args.credentials)))
    print(json.dumps({"answer_http": result["answer_http"], "embedding_http": result["embedding_http"],
                      "successful": sum(row["ok"] for row in result["answers"]), "seconds": result["seconds"]}))


if __name__ == "__main__":
    main()
