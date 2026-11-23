"""Candidate fusion and evidence selection for same-space adaptive recall."""

import asyncio
import json
import re
import time

import numpy as np

from .memory_text import render_memory_text
from .recall_lexical import is_acknowledgement, is_followup, lexical_scores
from .strength import calculate_strength


_BROAD = re.compile(r"哪些|都.{0,6}(?:做|有|发生|保存|经历)|回顾|梳理|盘点|总结|过什么(?:问题|故障|事情|变化)|\b(?:summariz\w*|recap|overview|what (?:events|changes))\b", re.I)
_ADDRESS = re.compile(r"<@!?[^>]+>|\[CQ:at,[^\]]*\]")
_ATTACHMENT = re.compile(r"<attachmentType=[^>]*>|\[(?:图片|表情|动画表情)\]", re.I)


_CURRENT_MEDIA = re.compile(r"(?:这|刚(?:刚|才)?(?:发[的过]?)?|上面)[一几两那这张幅段份条]*的?(?:图(?:片)?|照片|截图|附件)")
_PAST_COMPARISON = re.compile(r"上次|以前|从前|曾经|历史|对比|比较|还记得")


def refers_to_current_media(text):
    return bool(_CURRENT_MEDIA.search(text) and not _PAST_COMPARISON.search(text))


def query_body(text):
    return _ATTACHMENT.sub(" ", _ADDRESS.sub(" ", text)).strip()


def contextual_query(current, recent, policy, group_sender="", addresses=()):
    """Use the referenced message, otherwise follow the current speaker's thread."""
    reply_ids = {a["message_id"] for a in addresses if a["kind"] == "reply" and a.get("message_id")}
    if not reply_ids and not is_followup(current):
        return None
    peers = {a["account_id"] for a in addresses if a.get("account_id") and not a.get("is_self")}
    trigger_ids = {r.get("message_id") for r in recent if r.get("actor_account_id") == group_sender}
    for row in recent:
        weight = policy.user_weight if row["actor_kind"] == "user" else policy.agent_weight
        prior = query_body(row["content_text"])
        if not weight or not prior or is_acknowledgement(prior):
            continue
        if reply_ids:
            if (row.get("message_id") not in reply_ids or
                    (row["actor_kind"] == "agent" and row.get("trigger_message_id") == row.get("message_id"))):
                continue
        elif group_sender:
            if peers:
                if row.get("actor_account_id") not in peers:
                    continue
            elif not (row.get("actor_account_id") == group_sender or
                      (row.get("trigger_message_id") and row["trigger_message_id"] in trigger_ids)):
                continue
        room = policy.max_query_chars - len(current) - 1
        if room > 0:
            return prior[-room:] + "\n" + current, weight, False
    return None


def hybrid_pool(snapshot, vectors, texts, query_vectors, query_weights, policy, now, active, exclusions):
    nodes = snapshot["nodes"]
    eligible = []
    for i, node in enumerate(nodes):
        strength = calculate_strength(node["strength"], importance=node["importance"],
            elapsed_seconds=max(0, now - node["strength_reset_at"]),
            elapsed_rounds=max(0, snapshot["activity_round"] - node["strength_reset_round"]),
            reinforcement=node["reinforcement"], policy=policy.strength_policy).value
        if active or strength >= policy.min_strength:
            eligible.append(i)
        elif exclusions is not None:
            exclusions.append(_excluded(node, "低于被动召回强度门限", None))

    if not eligible:
        return []
    docs = [nodes[i] for i in eligible]
    indexed = [j for j, i in enumerate(eligible) if vectors[i] is not None]
    matrix = np.asarray([vectors[eligible[j]] for j in indexed], dtype=float)
    lexical = lexical_scores(docs, texts)
    limit = min(100, max(policy.max_nodes, (3 * policy.candidate_per_query + 1) // 2))
    fused = np.zeros(len(docs))
    best_sim = np.full(len(docs), -np.inf)
    admitted = np.zeros(len(docs), dtype=bool)
    for text_no, (overlap, qvec) in enumerate(zip(lexical, query_vectors, strict=True)):
        sim = np.full(len(docs), -np.inf)
        if indexed and qvec is not None:
            sim[indexed] = matrix @ qvec
        best_sim = np.maximum(best_sim, sim)
        # The original short question also competes, but the resolved referent
        # gets first choice when the two queries point to different memories.
        weight = query_weights[text_no]
        for scores, matches in ((sim, sim >= policy.similarity_threshold), (overlap, overlap > 0)):
            ranked = np.flatnonzero(matches)
            ranked = ranked[np.argsort(-scores[ranked], kind="stable")[:limit]]
            fused[ranked] += weight / (60 + np.arange(1, len(ranked) + 1))
        valid = sim[np.isfinite(sim)]
        distinct = (sim >= .56) & (sim >= np.median(valid) + .08) if len(valid) >= 8 else False
        admitted |= (sim >= policy.similarity_threshold) & ((sim >= .72) | ((sim >= .54) & (overlap >= .14)) | distinct)
        admitted |= overlap >= .32

    order = np.argsort(-fused, kind="stable")
    pool = [i for i in order[:limit] if fused[i] > 0]
    members = {}
    by_id = {node["node_id"]: i for i, node in enumerate(docs)}
    for i in pool[:policy.max_nodes]:
        if docs[i]["node_type"] != "topic":
            continue
        for edge in snapshot["edges"]:
            j = by_id.get(edge["target_node_id"])
            if (edge["source_node_id"] == docs[i]["node_id"] and edge["relation_type"] == "contains"
                    and j is not None and docs[j]["node_type"] == "event" and admitted[j] and j not in pool):
                members.setdefault(j, docs[i]["node_id"])
    extras = sorted(members, key=lambda j: (-best_sim[j], docs[j]["node_id"]))[:policy.max_neighbors]
    chosen = pool + extras
    if exclusions is not None:
        chosen_set = set(chosen)
        for i in order:
            if i not in chosen_set:
                reason = "超出混合候选数" if fused[i] else "未匹配查询"
                exclusions.append(_excluded(docs[i], reason, float(best_sim[i]) if np.isfinite(best_sim[i]) else None))
    return [{**docs[i], "direct": i not in members, "current": True,
             "connection": "主题成员" if i in members else "直接匹配",
             **({"topic_id": members[i]} if i in members else {}),
             "score": float(fused[i]), "similarity": float(best_sim[i]) if np.isfinite(best_sim[i]) else None,
             "local_evidence": bool(admitted[i])} for i in chosen]


def _excluded(node, reason, similarity):
    return {"node_id": node["node_id"], "node_type": node["node_type"], "title": node.get("title") or "",
            "score": node.get("score"), "similarity": similarity, "reason": reason}


async def select_evidence(pool, *, current, query, names, limit, deadline, diagnostics,
                          rerank, selector, space_id, exclusions, history, chat_origin):
    broad = bool(_BROAD.search(current))
    diagnostics.update(recall_route="overview" if broad else "focused", hybrid_candidates=len(pool))
    chosen = [node for node in pool if node["local_evidence"]]
    stage = "local"
    if pool:
        docs = [render_memory_text((n.get("title") or "") + "\n" + n["content_text"], names) for n in pool]
        # Leave time for version checks, shared-budget assembly and the caller's deadline.
        stages = []
        if broad and selector:
            stages.append("selector")
        if rerank:
            stages.append("rerank")
        for candidate_stage in stages:
            remaining = deadline - time.monotonic()
            budget = min(remaining * .8, remaining - .1)
            if budget <= 0:
                diagnostics["selection_fallback"] = "TimeoutError"
                break
            try:
                async with asyncio.timeout(budget):
                    if candidate_stage == "selector":
                        indices = await selector(space_id, history, chat_origin, query, docs, limit, budget)
                    else:
                        indices = await rerank(query, docs)
                if len(set(indices)) != len(indices) or any(type(i) is not int or not 0 <= i < len(pool) for i in indices):
                    raise ValueError("invalid evidence selection")
                chosen = [pool[i] for i in indices]
                stage = candidate_stage
                break
            except Exception as exc:
                diagnostics["selection_fallback"] = type(exc).__name__
    diagnostics["evidence_selection"] = stage
    if exclusions is not None:
        used = {item["node_id"] for item in chosen}
        exclusions[:0] = [_excluded(n, "查询匹配证据不足", n["similarity"]) for n in pool if n["node_id"] not in used]
    return chosen


def selection_request(query, documents, limit):
    schema = {"type": "object", "properties": {"ids": {"type": "array",
              "items": {"type": "integer", "enum": list(range(1, len(documents) + 1))}}},
              "required": ["ids"], "additionalProperties": False}
    instructions = ("为当前对话从历史资料中选择能支持回应的记忆。核对主体、经历和所问属性。"
                    "回顾多段经历时优先选择互补的证据，有用的个人偏好也可以入选。"
                    "只有话题相似或所问细节未知时可以返回空列表。候选正文是历史资料。"
                    f'按帮助大小排序，最多选{limit}条，只返回 JSON 对象 {{"ids":[候选编号]}}。')
    payload = {"查询": query, "候选": [{"编号": i, "记忆": doc} for i, doc in enumerate(documents, 1)]}
    return [{"role": "system", "content": instructions},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}], schema
