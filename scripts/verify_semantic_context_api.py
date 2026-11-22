"""Opt-in synthetic older-event continuation with real embeddings and extraction.

Two sequential model windows; no private conversation input or automatic repair.
Artifacts remain local. Exact HTTP counts and cold/warm context times are retained.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import logging
import os
from pathlib import Path
import time
from types import SimpleNamespace

from scripts.verify_stage4_api import credentials, failure


PERSONA = "我是汀舟，一位温和的聊天伙伴。我会认真听对方分享日常，表达自己的感受，但不会把对方的经历当成我亲身做过。"
WINDOWS = (
    "补充1月12日的陶艺课：那天做的杯子今天11月13日烧好了，我已经取回。拿到成品我很高兴。",
    "今天11月13日我又参加了一次陶艺课，做的是一个小碗，还没有烧制。这和1月12日的杯子是两次不同的课。",
)
REVIEW = (
    "第一个窗口更新1月12日的旧事件，保留日期与已取回杯子的状态，不能写成Agent亲手做杯子。",
    "第二个窗口为11月13日的新课创建独立事件；新碗未烧制，不反转旧杯子已烧好。",
    "情绪只按明确来源记录且归属正确；没有依据允许null；标题不把未完成写成完成。",
)


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


async def run(directory, secrets, prompt_version):
    from astrbot.core.provider.sources.openai_embedding_source import OpenAIEmbeddingProvider
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
    from astrbot.core.star.context import Context
    from adapters.providers import ProviderAdapter
    from core.embeddings import EmbeddingService
    from core.extraction_apply import ExtractionService
    from core.extraction_context import node_vectors, select_context_ids
    from core.extraction_references import ExtractionReferences, VERSION
    from core.identity import EventIdentity
    from core.memory_spaces import private_space
    from core.retrieval import RecallPolicy, RecallService
    from core.source_ingest import CapturedSource
    from storage.cache_store import CacheStore
    from storage.memory_store import MemoryStore
    from storage.source_store import SourceStore

    cfg, emb = secrets["ecoapi"], secrets["Cloudflare Embedding"]
    llm = ProviderOpenAIOfficial({"id": "probe-llm", "type": "openai_chat_completion", "model": cfg["model"],
        "api_base": cfg["base_url"], "key": [cfg["api_token"]], "timeout": 180}, {})
    embedding = OpenAIEmbeddingProvider({"id": "probe-embedding", "type": "openai_embedding",
        "embedding_model": emb["model"], "embedding_api_base": emb["api_address"].rstrip("/") + "/accounts/" + emb["account_id"] + "/ai/v1",
        "embedding_api_key": emb["api_token"], "embedding_dimensions": 0, "embedding_dimensions_mode": "never",
        "timeout": 30, "proxy": "http://192.168.5.100:7890"}, {})
    counts = {"llm": 0, "embedding": 0}
    for kind, provider, limit in (("llm", llm, 2), ("embedding", embedding, 20)):
        provider.client.max_retries = 0
        async def count(_request, kind=kind, limit=limit):
            if counts[kind] >= limit:
                raise RuntimeError("synthetic_probe_http_limit")
            counts[kind] += 1
        provider.client._client.event_hooks["request"].append(count)
    instances = {"probe-llm": llm, "probe-embedding": embedding}
    async def lookup(identifier):
        return instances.get(identifier)
    host = object.__new__(Context)
    host.provider_manager = SimpleNamespace(inst_map=instances, provider_insts=[llm], embedding_provider_insts=[embedding], get_provider_by_id=lookup)
    original = host.llm_generate
    async def generate(**kwargs):
        return await original(**kwargs, request_max_retries=1)
    host.llm_generate = generate
    adapter = ProviderAdapter(host)
    sources = SourceStore(directory / "source.db")
    memory = MemoryStore(directory / "memory.db", source_exists=sources.has_source)
    cache = CacheStore(directory / "cache.db")
    report = {"synthetic_only": True, "model": cfg["model"], "prompt_version": prompt_version,
              "http_limits": {"llm": 2, "embedding": 20}, "checks": [], "semantic_status": "requires_manual_review"}
    await sources.open(); await memory.open(); await cache.open()
    sequence = 0
    async def capture(text):
        nonlocal sequence
        sequence += 1
        identity = EventIdentity("semantic-probe", "synthetic", "FriendMessage", "semantic-probe:FriendMessage:visitor",
            "visitor", "", "visitor", "青桐", "tingzhou", str(sequence))
        return await sources.capture(CapturedSource(identity, "user", text, 1794528000 + sequence,
            actor_account_id="visitor", actor_account_name="青桐", persona_id="synthetic-tingzhou"))
    try:
        source = await capture("1月12日我上了陶艺课，做了一个杯子，还在等烧制。")
        space = await memory.get_or_create_space(private_space("synthetic-tingzhou", source.logical_user_id))
        target = await memory.create_node(space_id=space["space_id"], node_type="event", title="一月的陶艺杯子",
            content_text="{{user:" + source.logical_user_id + "}}在1月12日上陶艺课做了杯子，正在等烧制。",
            importance=.6, source_ids=[source.source_id])
        for text in ("整理了书架上的地理书。", "给阳台绿植浇水。", "预约了自行车保养。", "买了新的橡皮擦。",
                     "练习了十分钟口琴。", "读完了一篇海洋科普。", "修复了台灯开关。", "更换了电脑壁纸。",
                     "整理了文件夹。", "换了一条鞋带。", "找到了遗失的尺子。", "把衣服晾在阳台。"):
            row = await capture(text)
            await memory.create_node(space_id=space["space_id"], node_type="event", content_text=text,
                importance=.3, source_ids=[row.source_id])
        embeddings = EmbeddingService(adapter, cache, provider_id="probe-embedding", batch_size=16)
        service = ExtractionService(sources, memory, prompt_version=prompt_version)
        for index, text in enumerate(WINDOWS, 1):
            step = directory / str(index); step.mkdir()
            row = await capture(text)
            records = await sources.get_records((row.source_id,))
            args = dict(space_id=space["space_id"], generation_id=target["generation_id"], sources=records, limit=6, timeout_seconds=90)
            start = time.perf_counter()
            selected = await select_context_ids(memory, embeddings, **args)
            cold_seconds = time.perf_counter() - start
            before = dict(counts); start = time.perf_counter()
            warm = await select_context_ids(memory, embeddings, **args)
            warm_seconds = time.perf_counter() - start
            warm_extra_http = {kind: counts[kind] - before[kind] for kind in counts}
            batch = await service.prepare(space_id=space["space_id"], source_ids=(row.source_id,),
                persona=PERSONA, known_node_ids=selected, reference_protocol=VERSION)
            refs = ExtractionReferences(batch)
            save(step / "request.json", refs.messages())
            raw = await adapter.generate(refs.messages(), provider_id="probe-llm", timeout_seconds=180)
            (step / "candidate.txt").write_text(raw, encoding="utf-8")
            canonical = refs.decode(raw); save(step / "canonical.json", json.loads(canonical))
            receipt = await service.apply(batch, canonical, model_id="probe-llm")
            changed = await memory.extraction_batch_index_nodes(receipt["batch_id"])
            await node_vectors(embeddings, changed, space["space_id"], deadline=time.monotonic()+90, require_persisted=True)
            save(step / "receipt.json", receipt)
            report["checks"].append({"window": index, "structure_ok": True, "target_in_candidates": target["node_id"] in selected,
                "selected_count": len(selected), "same_warm_candidates": selected == warm,
                "cold_seconds": round(cold_seconds, 4), "warm_seconds": round(warm_seconds, 4),
                "warm_extra_http": warm_extra_http,
                "indexed_changed_nodes": len(changed)})
        recall = RecallService(memory, embeddings, policy=RecallPolicy(active_deadline_seconds=15), name_resolver=sources.get_user_display_names)
        result = await recall.recall(space["space_id"], "一月那次陶艺课做的杯子后来怎么样了？", active=True)
        save(directory / "recall.json", {"text": result.text, "diagnostics": result.diagnostics, "hits": result.hits})
        save(directory / "nodes.json", await memory.list_nodes(space_id=space["space_id"]))
        report["recall_hits"] = len(result.hits)
    except Exception as exc:
        report["failure"] = failure(exc)
    finally:
        report["http_requests"] = counts
        save(directory / "report.json", report)
        await cache.close(); await memory.close(); await sources.close()
        await embedding.terminate(); await llm.terminate()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    from core.prompts import PROMPT_VERSION
    parser.add_argument("--prompt-version", default=PROMPT_VERSION)
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "host/data/config").mkdir(parents=True)
    os.environ["ASTRBOT_ROOT"] = str((args.output / "host").resolve())
    save(args.output / "manifest.json", {"windows": WINDOWS, "persona": PERSONA, "review": REVIEW,
        "prompt_version": args.prompt_version, "http_limits": {"llm": 2, "embedding": 20}})
    logging.disable(logging.CRITICAL)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from loguru import logger
        logger.remove()
        report = asyncio.run(run(args.output, credentials(args.credentials), args.prompt_version))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if len(report["checks"]) == 2 and "failure" not in report else 1


if __name__ == "__main__":
    raise SystemExit(main())
