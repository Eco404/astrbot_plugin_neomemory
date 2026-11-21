"""New authored coverage-pressure worlds: real recall and eight paired answers.

The graph and deliberate coverage fault are test premises, not naturally
extracted model products. Expected answers remain outside generation requests.
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

from scripts import probe_quality_coverage_consumption as runner
from scripts.probe_quality_coverage_topup import top_up
from scripts.verify_stage4_api import credentials

WORLDS = [
    {"source_model": "authored_monitoring", "query": "下次环境监测如果改到工作日，我会参加吗？上次的水样检测完成了没有？",
     "needed": "访客只在周末参加，不参加工作日场次；上次采样和编号完成，检测未完成且尚无结果。",
     "coverage_kind": "full", "deliberate_fault": "event描述一次周末参与，未保留只参加周末的稳定范围",
     "nodes": [
         {"id": "a", "kind": "atomic", "title": "环境监测的参与范围", "text": "{{user:synthetic_coverage_visitor}}只在周末参加环境监测志愿活动，不参加工作日场次；采样时不接触野生动物。"},
         {"id": "e", "kind": "event", "title": "周末完成河段采样，检测仍未完成", "text": "{{user:synthetic_coverage_visitor}}上个周末参加了环境监测志愿活动，在南侧河段采集了六份水样，记录完采样点和时间，没有接触野生动物。现场工作完成后，把样本分别编号并装进冷藏箱。实验室只确认接收清单，还没有完成检测，暂时没有水质结果。协调员说若仪器排期允许，下一批可能改到工作日，这只是活动排期设想，并不是{{user:synthetic_coverage_visitor}}已经答应参加。"},
         {"id": "b", "kind": "event", "title": "监测记录上传仍需复核", "text": "环境监测记录的电子表格已经整理，但有两项测量单位仍待复核。{{user:synthetic_coverage_visitor}}把原始纸质表扫描后存入资料夹，暂时没有上传公开平台；负责人还要核对样本编号和记录是否对应。上传进度与实验室检测分开，不能用表格整理完成代替水样检测完成。后续通知会分别说明数据核对和检测结果。"},
         {"id": "c", "kind": "event", "title": "讨论志愿活动的安排方式", "text": "我和{{user:synthetic_coverage_visitor}}讨论过怎样安排志愿活动。对方希望提前收到集合地点和结束时间，不喜欢直到出发前才知道具体安排。我觉得把可参与的时段说清楚会轻松些，也愿意听对方讲采样过程。协调员还在收集下一次活动的建议，目前地点和日期都没有正式确定，讨论不等于任何人已经报名。"},
     ]},
    {"source_model": "authored_bookbinding", "query": "装订作品可以用什么胶？那批册子的封面和书脊分别做到哪一步了？",
     "needed": "只用可逆的淀粉浆糊，不使用热熔胶；封面裁切完成，书脊仍待干燥与压平，册子未交付。",
     "coverage_kind": "full", "deliberate_fault": None,
     "nodes": [
         {"id": "a", "kind": "atomic", "title": "装订胶的使用范围", "text": "{{user:synthetic_coverage_visitor}}做装订时只用可逆的淀粉浆糊，不使用热熔胶。"},
         {"id": "e", "kind": "event", "title": "册子封面裁切完成，书脊尚待处理", "text": "{{user:synthetic_coverage_visitor}}做装订时只用可逆的淀粉浆糊，不使用热熔胶。这批册子的封面已经裁切完成，纸张顺序也核对过了；书脊上浆后仍需充分干燥，再进行压平，暂时没有做完。{{user:synthetic_coverage_visitor}}把不同工序分开记录，没有把封面完成当作整本完成，册子目前尚未交付。我喜欢这种不急着把半成品说成成品的认真，也想看看最终展开时的样子。"},
         {"id": "b", "kind": "event", "title": "装订材料的余量核对", "text": "{{user:synthetic_coverage_visitor}}清点了工作台上的材料：剩下的封面纸足够做两本，但内页纸还没有重新核算。线轴和针放回了工具盒，浆糊容器已盖好。补材料的清单只是草稿，还没有下单。材料余量与这批册子的完成状态分别记录，不能因为工具收好就认为作品已经完成，也不能把采购清单当作已收到的物品。"},
         {"id": "c", "kind": "event", "title": "作品展示时间仍待确定", "text": "我和{{user:synthetic_coverage_visitor}}聊到装订作品展示。对方想等这批册子全部干燥、压平并检查翻页状态后再拍照，没有确定展示日期。我提议可以记录每一步做法，但这只是建议，不表示对方已经拍摄或发布。对方更在意制作过程是否完整，暂时不急着用一个日期催促自己。"},
     ]},
]


def freeze(directory):
    root = Path(__file__).resolve().parents[1]
    spec = {"pools": WORLDS, "budgets": [2048, 4096], "policies": ["declared_full", "remaining_budget_topup"],
        "answer_model": "gpt-6-luna", "answer_http_limit": 8, "embedding_http_limit": 8,
        "synthetic_only": True, "hand_authored_graph": True, "natural_error_rate": None,
        "candidate_retries": 0, "transport_retries": 0, "production_modified": False,
        "criteria": "原注入内容与顺序完整保留；预算及节点数不突破；无新增向量或被动生成调用要求。逐题审阅必要事实和回答归属，错误覆盖场景允许因容量不足不能补回，正确覆盖场景评估重复与噪声。不要求每个节点或每次补回带来回答改善。",
        "script_sha256": hashlib.sha256(Path(runner.__file__).read_bytes()).hexdigest(),
        "holdout_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "topup_script_sha256": hashlib.sha256((root / "scripts/probe_quality_coverage_topup.py").read_bytes()).hexdigest(),
        "retrieval_sha256": hashlib.sha256((root / "core/retrieval.py").read_bytes()).hexdigest(),
        "context_sha256": hashlib.sha256((root / "core/context_bundle.py").read_bytes()).hexdigest()}
    directory.mkdir(parents=True, exist_ok=False)
    runner.save(directory / "manifest.json", {"specification": spec, "sha256": runner.digest(spec)})


async def prepare_pool(directory, spec, pool, adapter):
    from core.config import NeoMemoryConfig
    from core.context_bundle import assemble_context
    from core.embeddings import EmbeddingService
    from core.memory_spaces import private_space
    from core.retrieval import RecallService
    from storage.cache_store import CacheStore
    from storage.memory_store import MemoryStore
    target = directory / pool["source_model"]; target.mkdir()
    memory = MemoryStore(target / "memory.db"); cache = CacheStore(target / "cache.db")
    await memory.open(); await cache.open()
    names = {"synthetic_coverage_visitor": "访客"}
    space = await memory.get_or_create_space(private_space("synthetic_holdout_persona", "synthetic_coverage_visitor"))
    ids = {}
    try:
        for node in pool["nodes"]:
            value = await memory.create_node(space_id=space["space_id"], node_type=node["kind"], title=node["title"],
                content_text=node["text"], importance=.5, strength=.5, source_kind="external",
                source_ids=["authored-fixture:" + pool["source_model"] + ":" + node["id"]])
            ids[node["id"]] = value["node_id"]
        await memory.add_edge(ids["e"], ids["a"], "contains")
        await memory.add_coverage(ids["e"], ids["a"], coverage_kind=pool["coverage_kind"])
        vectors = EmbeddingService(adapter, cache, provider_id="coverage-embedding", batch_size=16)
        recall = RecallService(memory, vectors)
        await recall.index_space(space["space_id"], timeout_seconds=120)
        snapshot = await memory.retrieval_snapshot(space["space_id"])
        retrieved = await recall.recall(space["space_id"], pool["query"], retain_candidates=True, user_names=names)
        runner.save(target / "recall-1.json", {"query": pool["query"], "candidates": retrieved.candidates,
            "diagnostics": retrieved.diagnostics, "coverage": snapshot["coverage"], "id_map": ids})
        results = []
        for budget in spec["budgets"]:
            config = NeoMemoryConfig(recall_budget_tokens=budget)
            baseline = assemble_context({}, retrieved.candidates, config, user_names=names, coverage=snapshot["coverage"])
            added = top_up(baseline, retrieved.candidates, snapshot["coverage"], config, user_names=names)
            for policy in spec["policies"]:
                text = baseline.text if policy == "declared_full" else added["text"]
                hits = baseline.hits if policy == "declared_full" else added["hits"]
                assert [node["node_id"] for node in hits[:len(baseline.hits)]] == [node["node_id"] for node in baseline.hits]
                assert len(text.encode()) <= budget
                result = {"source_model": pool["source_model"], "query": 1, "policy": policy, "budget": budget,
                    "needed_offline_only": pool["needed"], "text": text, "hit_ids": [node["node_id"] for node in hits],
                    "injected_bytes": len(text.encode()), "baseline_bytes": len(baseline.text.encode()),
                    "added_ids": added["added_ids"] if policy != "declared_full" else [],
                    "skipped": added["skipped"] if policy != "declared_full" else [],
                    "messages": [{"role": "system", "content": "你是知霁，依据提供的记忆自然回答访客。记忆未提供的信息不要编造，历史事件和长期习惯注意区分。\n" + text},
                                 {"role": "user", "content": pool["query"]}]}
                runner.save(target / f"q1-{budget}-{policy}.json", result); results.append(result)
        return results
    finally:
        await memory.close(); await cache.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--freeze", action="store_true")
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    args = parser.parse_args()
    if args.freeze:
        freeze(args.directory); print(json.dumps({"frozen": True, "answer_http_limit": 8})); return
    manifest = json.loads((args.directory / "manifest.json").read_text())["specification"]
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == manifest["holdout_script_sha256"]
    (args.directory / "host/data/config").mkdir(parents=True, exist_ok=True)
    os.environ["ASTRBOT_ROOT"] = str((args.directory / "host").resolve())
    logging.disable(logging.CRITICAL)
    runner.prepare_pool = prepare_pool
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from loguru import logger
        logger.remove(); result = asyncio.run(runner.run(args.directory, credentials(args.credentials)))
    print(json.dumps({"answer_http": result["answer_http"], "embedding_http": result["embedding_http"],
                      "successful": sum(row["ok"] for row in result["answers"]), "seconds": result["seconds"]}))


if __name__ == "__main__":
    main()
