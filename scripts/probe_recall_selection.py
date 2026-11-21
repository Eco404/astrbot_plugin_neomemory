#!/usr/bin/env python3
"""Evaluate passive recall on frozen synthetic queries using a shared embedding cache.

Run from the plugin root with PYTHONPATH=.:../AstrBot and the AstrBot environment.
Pass --embed once to obtain the fixture vectors using the configured test provider.
"""

import argparse
import asyncio
import contextlib
import hashlib
import io
import json
import logging
import os
import statistics
import time
from pathlib import Path

from scripts.verify_stage4_api import credentials, failure


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


async def fetch_vectors(fixture, cache_path, fixture_hash, args):
    from astrbot.core.provider.sources.openai_embedding_source import (
        OpenAIEmbeddingProvider,
    )

    from adapters.providers import normalize_vectors

    texts = []
    for split in fixture.values():
        texts.extend(node["content_text"] for node in split["nodes"])
        for case in split["cases"]:
            texts.append(case["query"])
            texts.extend(item["text"] for item in case["history"])
    texts = list(dict.fromkeys(texts))
    settings = credentials(args.credentials)["Cloudflare Embedding"]
    provider = OpenAIEmbeddingProvider(
        {
            "id": "recall-benchmark",
            "type": "openai_embedding",
            "embedding_model": settings["model"],
            "embedding_api_base": settings["api_address"].rstrip("/")
            + "/accounts/"
            + settings["account_id"]
            + "/ai/v1",
            "embedding_api_key": settings["api_token"],
            "embedding_dimensions": 0,
            "embedding_dimensions_mode": "never",
            "timeout": 30,
            "proxy": args.proxy,
        },
        {},
    )
    provider.client.max_retries = 0
    cache = {
        "model": settings["model"],
        "fixture_sha256": fixture_hash,
        "batches": [],
        "vectors": {},
    }
    try:
        for start in range(0, len(texts), 12):
            batch = texts[start : start + 12]
            started = time.perf_counter()
            vectors = normalize_vectors(
                await provider.get_embeddings(batch), count=len(batch)
            )
            cache["batches"].append(
                {"size": len(batch), "seconds": time.perf_counter() - started}
            )
            cache["vectors"].update(zip(batch, vectors, strict=True))
        save(cache_path, cache)
    finally:
        await provider.terminate()


def evaluate(split, vector_cache, strategy):
    from core.retrieval import RecallPolicy, rank_candidates, select_context

    policy = RecallPolicy()
    now = 1_800_000_000.0
    nodes = [
        {
            **node,
            "status": "active",
            "content_version": 1,
            "strength": 0.7,
            "importance": 0.7,
            "strength_reset_at": now,
            "strength_reset_round": 0,
            "reinforcement": 0,
        }
        for node in split["nodes"]
    ]
    edges = [
        {"source_node_id": topic, "target_node_id": member, "relation_type": "contains"}
        for topic, members in split["members"].items()
        for member in members
    ]
    snapshot = {"nodes": nodes, "edges": edges, "coverage": [], "activity_round": 0}
    vectors = [vector_cache[node["content_text"]] for node in nodes]
    rows = []
    for case in split["cases"]:
        texts = [case["query"], *(item["text"] for item in case["history"])]
        weights = [
            1.0,
            *(
                policy.user_weight if item["role"] == "user" else policy.agent_weight
                for item in case["history"]
            ),
        ]
        queries = [
            (vector_cache[text], weight, i == 0)
            for i, (text, weight) in enumerate(zip(texts, weights, strict=True))
        ]
        started = time.perf_counter()
        ranked = rank_candidates(
            snapshot,
            vectors,
            queries,
            policy,
            now,
            query_texts=texts,
            strategy=strategy,
        )
        hits = select_context(ranked, [], policy)
        elapsed = (time.perf_counter() - started) * 1000
        rows.append(
            {
                "case": case["id"],
                "kind": case["kind"],
                "relevant": case["relevant"],
                "candidate_direct": [
                    item["node_id"] for item in ranked if item["direct"]
                ],
                "selected": [item["node_id"] for item in hits],
                "ms": round(elapsed, 3),
            }
        )
    positives = [row for row in rows if row["relevant"]]
    broad = [row for row in positives if row["kind"] == "broad"]

    def overlap(row, field):
        return len(set(row[field]) & set(row["relevant"]))

    summary = {
        "positives": len(positives),
        "direct_oracle_hits": sum(
            bool(overlap(row, "candidate_direct")) for row in positives
        ),
        "final_hits": sum(bool(overlap(row, "selected")) for row in positives),
        "first_hits": sum(
            bool(row["selected"] and row["selected"][0] in row["relevant"])
            for row in positives
        ),
        "returned_relevant": sum(overlap(row, "selected") for row in rows),
        "returned_total": sum(len(row["selected"]) for row in rows),
        "positive_returned_total": sum(len(row["selected"]) for row in positives),
        "mean_returned": round(
            statistics.mean(len(row["selected"]) for row in rows), 3
        ),
        "broad_returned_relevant": sum(overlap(row, "selected") for row in broad),
        "broad_relevant": sum(len(row["relevant"]) for row in broad),
        "rank_select_ms_p50": round(statistics.median(row["ms"] for row in rows), 3),
    }
    for kind in ("negative_near", "negative_far"):
        subset = [row for row in rows if row["kind"] == kind]
        summary[kind] = {
            "empty": sum(not row["selected"] for row in subset),
            "total": len(subset),
        }
    for kind in ("specific", "paraphrase", "history", "broad"):
        subset = [row for row in positives if row["kind"] == kind]
        summary[kind] = {
            "hit": sum(bool(overlap(row, "selected")) for row in subset),
            "total": len(subset),
        }
    return {"summary": summary, "cases": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "tests/fixtures/recall-selection.json",
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--embed", action="store_true")
    parser.add_argument("--credentials", type=Path, default=Path("docs/api_key.md"))
    parser.add_argument("--proxy")
    parser.add_argument("--split", choices=("dev", "holdout"), required=True)
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    path = args.directory / "vectors.json"
    fixture_bytes = args.fixture.read_bytes()
    fixture = json.loads(fixture_bytes)
    fixture_hash = hashlib.sha256(fixture_bytes).hexdigest()
    if args.embed:
        root = args.directory / "host"
        (root / "data/config").mkdir(parents=True, exist_ok=True)
        os.environ["ASTRBOT_ROOT"] = str(root.resolve())
        logging.disable(logging.CRITICAL)
        try:
            with (
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                asyncio.run(fetch_vectors(fixture, path, fixture_hash, args))
        except Exception as exc:  # noqa: BLE001 - Provider errors may include credentials.
            raise SystemExit(json.dumps(failure(exc))) from None
    cache = json.loads(path.read_text(encoding="utf-8"))
    if fixture_hash != cache["fixture_sha256"]:
        raise ValueError("fixture and embedding cache differ")
    results = {
        strategy: evaluate(fixture[args.split], cache["vectors"], strategy)
        for strategy in ("baseline", "wide", "lexical", "gated")
    }
    data = {
        "fixture_sha256": fixture_hash,
        "cache_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "split": args.split,
        "model": cache["model"],
        "batches": cache["batches"],
        "strategies": results,
        "code_sha256": {
            str(source): hashlib.sha256(source.read_bytes()).hexdigest()
            for source in (
                Path("core/retrieval.py"),
                Path("core/recall_lexical.py"),
                Path(__file__),
            )
        },
    }
    save(args.directory / (args.split + ".json"), data)
    print(json.dumps({name: result["summary"] for name, result in results.items()}))


if __name__ == "__main__":
    main()
