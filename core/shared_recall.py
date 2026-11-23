"""Same-persona auxiliary memories: source-scoped search, attribution and final admission."""

import asyncio
import json
import time
from dataclasses import dataclass, replace

from .memory_text import memory_texts, render_display_text, render_memory_text, user_reference_ids
from .recall_lexical import is_acknowledgement
from .retrieval import rank_candidates, render_node, select_context


@dataclass
class SharedCandidate:
    node: dict
    space_id: str
    current_scene: str
    origin: str
    participants: list[str]
    names: dict[str, str]


def _person(user_id, names):
    return render_memory_text("{{user:" + user_id + "}}", names)


async def _scene(space, sources, names):
    if space["space_type"] == "private":
        return "与「" + _person(space["logical_user_id"], names) + "」的私聊"
    row = await sources.database.fetchone(
        "SELECT display_name FROM group_display_names WHERE platform_instance_id=? AND group_id=?",
        (space["platform_instance_id"], space["group_id"]))
    return "群聊「" + str(row[0] if row and row[0] else space["group_id"]) + "」"


async def search_shared(memory, sources, service, local, current, query, deadline, *,
                        preview=False, filters=None, current_names=None):
    """Search indexed nodes only; query computation belongs to the originating space."""
    if not query.strip() or (filters and filters.kind == "note") or (filters is None and is_acknowledgement(query)):
        return []
    spaces = await memory.list_shared_spaces(local.space_id)
    if not spaces:
        return []
    settings = await memory.get_space_policy(local.space_id)
    limit = settings["external_limit"]
    if not limit or not settings["external_budget_percent"]:
        return []
    query = query[:service.policy.max_query_chars]
    fingerprint = service.embeddings.fingerprint
    vector = local.query_vector if local.query_fingerprint == fingerprint else None
    candidate_limit = max(limit * 3, service.policy.candidate_per_query)
    policy = replace(service.policy, max_neighbors=0, ranking_strategy="gated")
    candidates = []
    for space in spaces:
        if time.monotonic() >= deadline:
            break
        sid = space["space_id"]
        snapshot = await memory.retrieval_snapshot(sid, include_historical=filters is not None, limit=policy.index_limit)
        nodes = snapshot["nodes"]
        if filters:
            nodes = snapshot["nodes"] = [n for n in nodes if filters.matches(n)]
        if not nodes:
            continue
        vectors = await service.embeddings.cached([n["content_text"] for n in nodes], namespace=sid,
            kind="node", identities=[(n["node_id"], n["content_version"]) for n in nodes], read_only=True)
        if all(v is None for v in vectors):
            continue
        if vector is None:
            if local.diagnostics.get("embedding_degraded") or local.diagnostics.get("embedding_changed"):
                break
            vector, = await service.embeddings.embed([query], namespace=local.space_id,
                timeout_seconds=max(.001, deadline - time.monotonic()), read_only=preview)
        if service.embeddings.fingerprint != fingerprint or vector is None:
            return []
        ranked = await asyncio.to_thread(rank_candidates, snapshot, vectors, [(vector, 1., True)],
            policy, time.time(), active=False, query_texts=[query])
        direct = [n for n in ranked if n["direct"]]
        for node in select_context(direct, snapshot["coverage"], fits=lambda items: len(items) <= candidate_limit,
                                   distinct_evidence=filters is None):
            candidates.append((node, space))
        candidates.sort(key=lambda entry: (-entry[0]["score"], entry[0]["node_id"]))
        # Keep alternatives for budget and final visibility checks.
        del candidates[candidate_limit:]
    if not candidates or service.embeddings.fingerprint != fingerprint:
        return []

    current = current or await memory.get_space(local.space_id)
    if current is None:
        return []
    node_participants = {}
    ids = set(current_names or ())
    for node, space in candidates:
        participants = await memory.list_node_participants(node["node_id"])
        member_ids = [p["participant_id"] for p in participants if p["participant_kind"] == "logical_user"]
        node_participants[node["node_id"]] = participants
        ids.update(member_ids)
        ids.update(user_reference_ids(memory_texts(node)))
        if space.get("logical_user_id"):
            ids.add(space["logical_user_id"])
    if current.get("logical_user_id"):
        ids.add(current["logical_user_id"])
    names = await sources.get_user_display_names(ids)
    names.update(current_names or {})
    current_scene = await _scene(current, sources, names)
    scenes = {}
    prepared = []
    for node, space in candidates:
        sid = space["space_id"]
        if sid not in scenes:
            scenes[sid] = await _scene(space, sources, names)
        participants = [
            _person(p["participant_id"], names) if p["participant_kind"] == "logical_user"
            else p["display_name"] or p["participant_id"] for p in node_participants[node["node_id"]]
        ]
        prepared.append(SharedCandidate(node, sid, current_scene, scenes[sid], participants, names))
    return prepared


async def deliver_shared(memory, local_text, prepared, *, space_id, budget, filters=None, remaining=None):
    """Recheck each version and grant after expensive work, then fit whole attributed rows."""
    if not prepared:
        return local_text, []
    versions = {}
    for candidate in prepared:
        node = candidate.node
        bucket = versions.setdefault((candidate.space_id, node["generation_id"]), {})
        bucket[node["node_id"]] = node["content_version"]
    valid = {}
    for (sid, generation), expected in versions.items():
        nodes = await memory.revalidate_recall(sid, generation, expected, include_historical=filters is not None)
        valid.update((node["node_id"], node) for node in nodes if not filters or filters.matches(node))

    # Read grants after ranking and version checks; a source can opt out during either.
    settings = await memory.get_space_policy(space_id)
    allowed = {s["space_id"] for s in await memory.list_shared_spaces(space_id)}
    limit = settings["external_limit"]
    if remaining is not None:
        limit = min(limit, remaining)
    room = min(budget - len(local_text.encode()), budget * settings["external_budget_percent"] // 100)
    if limit <= 0 or room <= 0:
        return local_text, []
    current_scene = json.dumps(prepared[0].current_scene, ensure_ascii=False)
    prefix = (
        ("\n" if local_text else "") + f"其他对话中的记忆（当前场景：{current_scene}）。"
        "以下片段是同一人格在其他空间的历史资料。提及往事时保留来源场景和参与者，"
        "只有当前对话提供独立依据时，才可将经历归于当前对象。\n"
    )
    size = len(prefix.encode()) + 2
    hits, parts = [], []
    for candidate in prepared:
        node_id = candidate.node["node_id"]
        if candidate.space_id not in allowed or node_id not in valid:
            continue
        hit = {**candidate.node, **valid[node_id]}
        data = json.loads(render_node(hit, candidate.names))
        data.update(source_scene=candidate.origin, source_space_id=candidate.space_id,
                    participants=candidate.participants)
        encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        extra = len(encoded.encode()) + bool(parts)
        if size + extra > room:
            continue
        hit.update(external_space_id=candidate.space_id, source_scene=candidate.origin,
                   title=render_display_text(hit.get("title") or "", candidate.names))
        hits.append(hit)
        parts.append(encoded)
        size += extra
        if len(hits) == limit:
            break
    return (local_text + prefix + "[" + ",".join(parts) + "]" if hits else local_text), hits
