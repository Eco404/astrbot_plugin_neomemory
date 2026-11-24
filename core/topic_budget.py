"""Fit optional source excerpts to the actual serialized topic request budget."""

from __future__ import annotations

from copy import deepcopy

from .topic_evidence import MAX_EXCERPT_UNITS, clip_topic_excerpt
from .topic_prompt import PROMPT_VERSION, build_messages


def message_bytes(messages):
    return sum(len(message["content"].encode("utf-8")) for message in messages)


def fit_topic_evidence(persona, nodes, edges, evidence, max_bytes, *, related_topics=(),
                       allowed_user_ids=None, prompt_version=PROMPT_VERSION):
    """Keep complete nodes; excerpts may shorten, identities and provenance may not.

    The caller orders evidence by seed priority. Round-robin source selection
    gives each node an opportunity before adding a second citation to a node.
    No empty evidence row is emitted, and every retained row remains incomplete.
    """
    options = {"related_topics": related_topics, "allowed_user_ids": allowed_user_ids,
               "prompt_version": prompt_version}
    base = message_bytes(build_messages(persona, nodes, edges, **options))

    def has_excerpt(source):
        return bool(source.get("content_text") or source.get("forwarded_messages"))

    available = sum(has_excerpt(source) for row in evidence for source in row["sources"])
    if base > max_bytes:
        return None, {"base_prompt_bytes": base, "prompt_bytes": base,
                      "source_excerpts_available": available, "source_excerpts_retained": 0}
    selected = []
    by_node = {}
    retained = 0
    shortened = 0
    minimum_with_evidence = None

    def append(row, source):
        node_id = row["node_id"]
        if node_id not in by_node:
            by_node[node_id] = {"node_id": node_id, "sources": [], "complete": False}
            selected.append(by_node[node_id])
        by_node[node_id]["sources"].append(source)

    def remove(row):
        current = by_node[row["node_id"]]
        current["sources"].pop()
        if not current["sources"]:
            selected.remove(current)
            del by_node[row["node_id"]]

    depth = max((len(row["sources"]) for row in evidence), default=0)
    for index in range(depth):
        for row in evidence:
            if index >= len(row["sources"]) or not has_excerpt(row["sources"][index]):
                continue
            source = deepcopy(row["sources"][index])
            original = row["sources"][index]
            append(row, source)
            if message_bytes(build_messages(persona, nodes, edges, evidence=selected, **options)) > max_bytes:
                low, high = 0, MAX_EXCERPT_UNITS if "forwarded_messages" in original else len(original["content_text"])
                while low < high:
                    length = (low + high + 1) // 2
                    source.update(clip_topic_excerpt(original, length)[0])
                    if message_bytes(build_messages(persona, nodes, edges, evidence=selected, **options)) <= max_bytes:
                        low = length
                    else:
                        high = length - 1
                if low == 0:
                    if not retained:
                        source.update(clip_topic_excerpt(original, 1)[0])
                        needed = message_bytes(build_messages(persona, nodes, edges, evidence=selected, **options))
                        minimum_with_evidence = needed if minimum_with_evidence is None else min(needed, minimum_with_evidence)
                    remove(row)
                    continue
                source.update(clip_topic_excerpt(original, low)[0])
                shortened += 1
            retained += 1
    actual = message_bytes(build_messages(persona, nodes, edges, evidence=selected, **options))
    if not retained and minimum_with_evidence is not None:
        actual = minimum_with_evidence
    diagnostics = {"base_prompt_bytes": base, "prompt_bytes": actual,
        "source_excerpts_available": available, "source_excerpts_retained": retained,
        "source_excerpts_omitted": available - retained, "source_excerpts_shortened": shortened}
    # A request that had attribution evidence must not silently become a prose-only request.
    return (selected if retained or not available else None), diagnostics
