"""Window-local model references with exact restoration of authoritative IDs."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .extraction_schema import parse_extraction_output_for_context
from .group_addressing import address_snapshot_users
from .memory_text import map_user_references, memory_texts, user_reference_ids

if TYPE_CHECKING:
    from .extraction_apply import PreparedExtraction

VERSION = "reference.v1"
_USER_FIELDS = (
    "logical_user_id", "actor_logical_user_id", "conversation_logical_user_id",
)


class ReferenceValidationError(ValueError):
    """A model-selected reference is absent from the fixed input dictionary."""

    code = "unknown_reference"

    def __init__(self, path: str, kind: str):
        self.path = path
        self.kind = kind
        super().__init__(f"{path}: unknown {kind} reference")


class ExtractionReferences:
    """Encode identifiers and explicit user placeholders in derived prose."""

    def __init__(self, batch: PreparedExtraction):
        self.batch = batch
        source_ids = [source["source_id"] for source in batch.sources]
        node_ids = [node["node_id"] for node in batch.known_nodes]
        user_ids = []
        contexts = [batch.space, *batch.sources, *batch.profiles]
        if batch.relationship is not None:
            contexts.append(batch.relationship)
        for item in contexts:
            for field in _USER_FIELDS:
                value = item.get(field)
                if value:
                    user_ids.append(value)
        for item in (*batch.known_nodes, *batch.profiles,
                     *((batch.relationship,) if batch.relationship else ())):
            user_ids.extend(user_reference_ids(memory_texts(item)))
        if batch.space.get("space_type") == "group":
            user_ids.extend(address_snapshot_users(batch.sources))
        self._forward = {
            kind: {value: f"{prefix}{index}" for index, value in enumerate(
                dict.fromkeys(values), start=1,
            )}
            for kind, prefix, values in (
                ("source", "s", source_ids), ("node", "n", node_ids),
                ("user", "u", user_ids),
            )
        }
        self._reverse = {
            kind: {alias: value for value, alias in mapping.items()}
            for kind, mapping in self._forward.items()
        }

    def _reference(self, value: str, kind: str, path: str, *, decode: bool) -> str:
        mapping = self._reverse if decode else self._forward
        if value not in mapping[kind]:
            raise ReferenceValidationError(path, kind)
        return mapping[kind][value]

    def messages(self) -> list[dict[str, str]]:
        messages = self.batch.messages()
        for message in messages:
            if message["role"] != "user":
                continue
            payload = json.loads(message["content"])
            contexts = []
            if payload.get("space") is not None:
                contexts.append(("$.space", payload["space"]))
            for index, source in enumerate(payload["sources"]):
                path = f"$.sources[{index}]"
                source["source_id"] = self._reference(
                    source["source_id"], "source", path + ".source_id", decode=False,
                )
                contexts.append((path, source))
                for target_index, target in enumerate(source.get("group_addressing", [])):
                    contexts.append((f"{path}.group_addressing[{target_index}]", target))
            for index, node in enumerate(payload["known_nodes"]):
                node["node_id"] = self._reference(
                    node["node_id"], "node", f"$.known_nodes[{index}].node_id",
                    decode=False,
                )
                self._prose(node, f"$.known_nodes[{index}]", decode=False)
            for index, edge in enumerate(payload.get("known_edges", [])):
                for field in ("source_node_id", "target_node_id"):
                    edge[field] = self._reference(edge[field], "node",
                        f"$.known_edges[{index}].{field}", decode=False)
            contexts.extend(
                (f"$.known_profiles[{index}]", profile)
                for index, profile in enumerate(payload["known_profiles"])
            )
            if payload.get("known_relationship") is not None:
                contexts.append(("$.known_relationship", payload["known_relationship"]))
            for index, profile in enumerate(payload["known_profiles"]):
                self._prose(profile, f"$.known_profiles[{index}]", decode=False)
            if payload.get("known_relationship") is not None:
                self._prose(payload["known_relationship"], "$.known_relationship", decode=False)
            for path, item in contexts:
                for field in _USER_FIELDS:
                    if item.get(field):
                        item[field] = self._reference(
                            item[field], "user", path + "." + field, decode=False,
                        )
            message["content"] = json.dumps(payload, ensure_ascii=False)
        return messages

    def _prose(self, item: dict[str, Any], path: str, *, decode: bool) -> None:
        for field in ("content", "content_text", "title", "description", "state_text", "event_time_text"):
            if isinstance(item.get(field), str):
                item[field] = map_user_references(item[field], lambda value: self._reference(
                    value, "user", path + "." + field, decode=decode))
        for slot in ("long_term", "aftereffect"):
            if isinstance(item.get(slot), dict) and isinstance(item[slot].get("body"), str):
                item[slot]["body"] = map_user_references(item[slot]["body"], lambda value: self._reference(
                    value, "user", path + "." + slot + ".body", decode=decode))
        if isinstance(item.get("state"), dict):
            self._prose(item["state"], path + ".state", decode=decode)
        if item.get("event_time") is not None:
            time_text = item["event_time"].get("text")
            if time_text is not None:
                item["event_time"]["text"] = map_user_references(time_text, lambda value: self._reference(
                    value, "user", path + ".event_time.text", decode=decode))
        if item.get("emotion") is not None:
            item["emotion"]["description"] = map_user_references(item["emotion"]["description"],
                lambda value: self._reference(value, "user", path + ".emotion.description", decode=decode))
        if "tags" in item:
            item["tags"] = [map_user_references(tag, lambda value: self._reference(
                value, "user", f"{path}.tags[{index}]", decode=decode)) for index, tag in enumerate(item["tags"])]

    def _output(self, raw: str, *, decode: bool) -> str:
        output = parse_extraction_output_for_context(
            raw, self.batch, validate_supplied_versions=False
        ).model_dump(mode="json")

        def evidence(item: dict[str, Any], path: str) -> None:
            for field in ("source_ids", "context_source_ids", "confirmation_source_ids"):
                if field in item:
                    item[field] = [
                        self._reference(value, "source", f"{path}.{field}[{index}]", decode=decode)
                        for index, value in enumerate(item[field])
                    ]

        def endpoint(item: dict[str, Any], field: str, path: str) -> None:
            value = item[field]
            if not value.startswith("local_"):
                item[field] = self._reference(value, "node", path + "." + field, decode=decode)

        for index, node in enumerate(output["node_changes"]):
            path = f"$.node_changes[{index}]"
            self._prose(node, path, decode=decode)
            evidence(node, path)
            if "base_node_ids" in node:
                node["base_node_ids"] = [
                    self._reference(value, "node", f"{path}.base_node_ids[{base_index}]", decode=decode)
                    for base_index, value in enumerate(node["base_node_ids"])
                ]
            if node["node_id"] is not None:
                node["node_id"] = self._reference(
                    node["node_id"], "node", path + ".node_id", decode=decode,
                )
        for index, edge in enumerate(output["edges"]):
            path = f"$.edges[{index}]"
            evidence(edge, path)
            endpoint(edge, "from_ref", path)
            endpoint(edge, "to_ref", path)
        for index, coverage in enumerate(output.get("coverage_changes", [])):
            path = f"$.coverage_changes[{index}]"
            evidence(coverage, path)
            endpoint(coverage, "event_ref", path)
            endpoint(coverage, "atomic_ref", path)
        profiles = [(f"$.profile_changes[{index}]", profile)
                    for index, profile in enumerate(output["profile_changes"])]
        if output["relationship_change"] is not None:
            profiles.append(("$.relationship_change", output["relationship_change"]))
        for path, item in profiles:
            self._prose(item, path, decode=decode)
            evidence(item, path)
            item["logical_user_id"] = self._reference(
                item["logical_user_id"], "user", path + ".logical_user_id", decode=decode,
            )
        return json.dumps(output, ensure_ascii=False)

    def decode(self, raw: str) -> str:
        """Return a canonical candidate; an unknown alias is never repaired."""
        return self._output(raw, decode=True)

    def encode_output(self, raw: str) -> str:
        """Encode a canonical fixture through the same strict dictionaries."""
        return self._output(raw, decode=False)
