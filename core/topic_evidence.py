"""Bounded, version-linked speaker evidence for topic attribution."""

from __future__ import annotations

from .forwarded_content import project_forward
from .group_addressing import group_source_addresses


MAX_EXCERPT_UNITS = 600


def clip_topic_excerpt(source, limit):
    """Shorten visible text while keeping each quoted author and nesting boundary."""
    text = source["content_text"][:limit]
    remaining = limit - len(text)
    truncated = len(text) < len(source["content_text"])

    def quote_prefix(messages):
        nonlocal remaining, truncated
        result = []
        for message in messages:
            if not remaining:
                truncated = True
                break
            # A card or an empty parent with nested messages also counts as evidence.
            remaining -= 1
            quoted = message.copy()
            if "text" in quoted:
                quoted["text"] = message["text"][:remaining]
                remaining -= len(quoted["text"])
                truncated |= len(quoted["text"]) < len(message["text"])
            if "forwarded_messages" in quoted:
                quoted["forwarded_messages"] = quote_prefix(message["forwarded_messages"])
            result.append(quoted)
        return result

    excerpt = source | {"content_text": text}
    if "forwarded_messages" in source:
        excerpt["forwarded_messages"] = quote_prefix(source["forwarded_messages"])
    excerpt["truncated"] = truncated or source.get("truncated", False)
    return excerpt, limit - remaining


async def collect_topic_evidence(memory, sources, nodes, seed_ids=(), *, char_budget=16000):
    """Reuse each source read while retaining direct versus inherited provenance.

    A bounded excerpt is context, not a replacement for its source. Missing or
    truncated evidence never authorizes inference about who experienced an event.
    """
    ordered = sorted(nodes, key=lambda node: (node["node_id"] not in seed_ids, node["node_type"] == "topic"))
    records, derivations, evidence = {}, {}, []
    for node in ordered:
        if char_budget <= 0:
            break
        if node["node_type"] == "topic":
            continue
        queue = [(node["node_id"], node["content_version"])]
        visited, citations = set(), []
        while queue and len(visited) < 12 and len(citations) < 4:
            identity = queue.pop(0)
            if identity in visited:
                continue
            visited.add(identity)
            if identity not in derivations:
                derivations[identity] = await memory.list_version_derivations("node", *identity)
            for derivation in reversed(derivations[identity]):
                if derivation["operation"] in {"title_update", "membership"}:
                    continue
                for citation in derivation["sources"]:
                    if citation["source_kind"] == "source_record" and citation["source_id"] not in [c[0] for c in citations]:
                        citations.append((citation["source_id"], identity))
                        if len(citations) == 4:
                            break
                for base in derivation["base_versions"]:
                    if base["kind"] == "node":
                        queue.append((base["entity_id"], base["version_no"]))
                if len(citations) == 4:
                    break
        missing = [source_id for source_id, _ in citations if source_id not in records]
        if missing:
            records.update({source_id: None for source_id in missing})
            available = [source_id for source_id in missing if await sources.has_source(source_id)]
            if available:
                records.update({item["source_id"]: item for item in await sources.get_records(tuple(available))})
        # Older sources can corroborate each other within the same member version.
        # A later, independent member cannot establish who owned a past account.
        forwards = {}
        for source_id, _ in citations:
            source = records.get(source_id)
            if source and (forward := project_forward(source)):
                forwards[source_id] = forward
        grouped = {}
        for source_id, basis in citations:
            source = records.get(source_id)
            if source and source.get("message_type", "").lower() == "groupmessage":
                if source_id in forwards:
                    source = source | {"content_text": forwards[source_id]["sender_comment"]}
                grouped.setdefault(basis, []).append(source)
        addresses = {source_id: projected for group in grouped.values()
                     for source_id, projected in group_source_addresses(group).items()}
        excerpts = []
        for source_id, basis in citations:
            source = records.get(source_id)
            if source is None or char_budget <= 0:
                continue
            fields = {key: source.get(key) for key in (
                "source_id", "actor_kind", "actor_logical_user_id", "conversation_logical_user_id", "observed_at")}
            original = source["content_text"]
            if source_id in forwards:
                original = forwards[source_id]["sender_comment"]
                fields["forwarded_messages"] = forwards[source_id]["forwarded_messages"]
            if source_id in addresses:
                fields.pop("conversation_logical_user_id")
                fields["group_message"] = True
                projected = addresses[source_id]
                if projected:
                    original = projected["content_text"]
                    fields["group_addressing"] = projected["group_addressing"]
            excerpt, used = clip_topic_excerpt(fields | {"content_text": original}, min(MAX_EXCERPT_UNITS, char_budget))
            char_budget -= used
            excerpts.append(excerpt | {"basis_node_id": basis[0], "basis_version": basis[1]})
        evidence.append({"node_id": node["node_id"], "sources": excerpts,
                         "complete": False})
    return evidence
