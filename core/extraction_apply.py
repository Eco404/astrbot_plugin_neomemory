"""Offline extraction orchestration; models propose, storage commits atomically."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .forwarded_content import project_forward
from .group_addressing import group_source_addresses
from .extraction_observations import build_observation_anchors
from .extraction_schema import parse_extraction_output
from .prompts import (
    PROMPT_VERSION,
    build_extraction_prompt,
    resolve_extraction_prompt_version,
    schema_for_prompt_version,
)


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()


@dataclass(frozen=True)
class PreparedExtraction:
    """Trusted input snapshot. This is application context, never model output."""

    batch_id: str
    space: dict[str, Any]
    generation_id: str
    expected_round: int
    sources: tuple[dict[str, Any], ...]
    known_nodes: tuple[dict[str, Any], ...]
    profiles: tuple[dict[str, Any], ...]
    relationship: dict[str, Any] | None
    persona: str
    job_id: str | None = None
    lease_token: str | None = None
    allow_building: bool = False
    source_cursor: int = 0
    prompt_version: str = PROMPT_VERSION
    reference_protocol: str | None = None
    known_edges: tuple[dict[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if self.prompt_version is None:
            raise ValueError("prepared extraction requires an explicit prompt version")
        schema_version = schema_for_prompt_version(self.prompt_version)
        selected = resolve_extraction_prompt_version(
            schema_version=schema_version, prompt_version=self.prompt_version
        )
        if selected != self.prompt_version:
            raise ValueError("prepared extraction requires an explicit prompt version")

    @property
    def input_digest(self) -> str:
        schema_version = schema_for_prompt_version(self.prompt_version)
        value = {
            "space": self.space,
            "generation": self.generation_id,
            "round": self.expected_round,
            "sources": self.sources,
            "nodes": self.known_nodes,
            "profiles": self.profiles,
            "relationship": self.relationship,
            "persona": self.persona,
            "prompt": self.prompt_version,
        }
        value["schema"] = schema_version
        if self.job_id:
            # Recall reinforcement is concurrent with background extraction and
            # never changes model-visible facts or mutation preconditions.
            value["nodes"] = [{key: item for key, item in node.items()
                if key not in {"reinforcement", "last_recalled_at"}} for node in self.known_nodes]
        if self.allow_building or self.source_cursor > 0:
            value.update(allow_building=self.allow_building, source_cursor=self.source_cursor)
        if self.reference_protocol is not None:
            value["reference_protocol"] = self.reference_protocol
        if self.known_edges:
            value["edges"] = self.known_edges
        return digest(value)

    def messages(self) -> list[dict[str, str]]:
        def select(item: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
            return {field: item[field] for field in fields if field in item}

        observation_fields = ("observation_anchors",)
        sources = self.sources
        projected = []
        for source in sources:
            forward = project_forward(source)
            if forward is not None:
                source = source | {"content_text": forward["sender_comment"],
                                   "forwarded_messages": forward["forwarded_messages"]}
            projected.append(source)
        sources = projected
        group_sources = {}
        if self.space.get("space_type") == "group":
            group_sources = group_source_addresses(sources, use_snapshots=True)
        return build_extraction_prompt(
            prompt_version=self.prompt_version,
            schema_version=schema_for_prompt_version(self.prompt_version),
            persona=self.persona,
            space=select(
                self.space,
                (
                    "space_id",
                    "space_type",
                    "persona_id",
                    "logical_user_id",
                    "platform_instance_id",
                    "group_id",
                ),
            ),
            sources=[
                select(
                    source,
                    (
                        "source_id",
                        "actor_kind",
                        "actor_logical_user_id",
                        *(("conversation_logical_user_id",) if not group_sources else ()),
                        "content_text",
                        "forwarded_messages",
                        "observed_at",
                        "event_time_start",
                        "event_time_end",
                        "event_time_text",
                        "stream_id",
                        "sequence_no",
                    ),
                )
                | group_sources.get(source["source_id"], {})
                for source in sources
            ],
            known_nodes=[
                select(
                    node,
                    (
                        "node_id",
                        "node_type",
                        "status",
                        "manual_protected",
                        "content_version",
                        "content_text",
                        "importance",
                        "event_time_start",
                        "event_time_end",
                        "event_time_text",
                        "title", "emotion",
                    ) + observation_fields,
                )
                for node in self.known_nodes
            ],
            known_edges=self.known_edges,
            known_profiles=[
                select(
                    profile,
                    (
                        "logical_user_id",
                        "tags",
                        "description",
                        "content_version",
                        "status",
                    ) + observation_fields,
                )
                for profile in self.profiles
            ],
            known_relationship=select(
                self.relationship,
                ("logical_user_id", "state_text", "state", "content_version", "status") + observation_fields,
            )
            if self.relationship
            else None,
        )


class ExtractionService:
    """An explicit-call service: no network provider, scheduler, or hidden retry."""

    def __init__(
        self, source_store: Any, memory_store: Any, *, prompt_version: str = PROMPT_VERSION
    ):
        self.sources = source_store
        self.memory = memory_store
        self.prompt_version = resolve_extraction_prompt_version(
            schema_version=schema_for_prompt_version(prompt_version), prompt_version=prompt_version
        )

    async def prepare(
        self,
        *,
        space_id: str,
        source_ids: tuple[str, ...],
        persona: str,
        known_node_ids: tuple[str, ...] = (),
        generation_id: str | None = None,
        job_id: str | None = None,
        lease_token: str | None = None,
        allow_building: bool = False,
        source_cursor: int = 0,
        reference_protocol: str | None = None,
    ) -> PreparedExtraction:
        space = await self.memory.get_space(space_id)
        if not space:
            raise ValueError("unknown memory space")
        if type(source_cursor) is not int or source_cursor < 0:
            raise ValueError("source_cursor must be a nonnegative integer")
        generation = await self._generation(
            space_id=space_id, generation_id=generation_id, job_id=job_id,
            lease_token=lease_token, allow_building=allow_building,
        )
        sources = await self.sources.get_records(source_ids)
        previous_sequence: dict[str, int] = {}
        for source in sources:
            if (
                source["identity_pending"]
                or source["persona_id"] != space["persona_id"]
            ):
                raise ValueError("unresolved or different source persona")
            if source["source_type"] != "conversation":
                raise ValueError(
                    "source_event extraction is reserved for a later version"
                )
            if source["actor_kind"] not in {"user", "agent"}:
                raise ValueError("unsupported source actor for conversation extraction")
            if not source.get("delivered_proactive"):
                if source["sequence_no"] <= previous_sequence.get(source["stream_id"], 0):
                    raise ValueError("sources must follow stream sequence order")
                previous_sequence[source["stream_id"]] = source["sequence_no"]
            if space["space_type"] == "private":
                owner = (
                    source["actor_logical_user_id"]
                    if source["actor_kind"] == "user"
                    else source["conversation_logical_user_id"]
                )
                if (
                    source["message_type"].lower() != "friendmessage"
                    or owner != space["logical_user_id"]
                ):
                    raise ValueError("source belongs to another private space")
            elif (
                source["message_type"].lower() != "groupmessage"
                or source["platform_instance_id"] != space["platform_instance_id"]
                or source["group_id"] != space["group_id"]
            ):
                raise ValueError("source belongs to another group space")
        if len(known_node_ids) > 50 or len(set(known_node_ids)) != len(known_node_ids):
            raise ValueError("known nodes require at most 50 unique IDs")
        nodes, edges = await self._topic_context(space_id, generation["generation_id"], known_node_ids)
        profiles = await self.memory.list_profiles(
            space_id=space_id, generation_id=generation["generation_id"]
        )
        # Only the people speaking in this window need their profiles in a group prompt.
        speakers = {
            source["actor_logical_user_id"]
            for source in sources
            if source["actor_kind"] == "user"
        }
        profiles = [
            profile for profile in profiles if profile["logical_user_id"] in speakers
        ]
        relationship = None
        if space["space_type"] == "private":
            relationship = await self.memory.get_relationship(
                space_id=space_id,
                logical_user_id=space["logical_user_id"],
                generation_id=generation["generation_id"],
            )
        entities = [("node", node["node_id"], node["content_version"]) for node in nodes]
        entities += [("profile", profile["profile_id"], profile["content_version"]) for profile in profiles]
        if relationship:
            entities.append(("relationship", relationship["relationship_id"], relationship["content_version"]))
        anchors = await build_observation_anchors(self.memory, entities)
        nodes = [node | {"observation_anchors": anchors["node", node["node_id"]]} for node in nodes]
        profiles = [profile | {"observation_anchors": anchors["profile", profile["profile_id"]]} for profile in profiles]
        if relationship:
            relationship = relationship | {"observation_anchors": anchors["relationship", relationship["relationship_id"]]}
        batch_id = "batch_" + digest(
            [space_id, generation["generation_id"], source_ids]
        )
        return PreparedExtraction(
            batch_id,
            space,
            generation["generation_id"],
            generation["activity_round"],
            tuple(sources),
            tuple(nodes),
            tuple(profiles),
            relationship,
            persona,
            job_id,
            lease_token,
            allow_building,
            source_cursor,
            prompt_version=self.prompt_version,
            reference_protocol=reference_protocol,
            known_edges=tuple(edges),
        )

    async def _topic_context(self, space_id, generation_id, node_ids):
        """Keep topic membership complete; the caller's prompt budget bounds expanded input."""
        nodes = {}
        edges = []
        async with self.memory.database.transaction(write=False) as connection:
            async def add(node_id):
                if node_id in nodes:
                    return
                row = await self.memory._node_row_in_connection(connection, node_id)
                if not row or row["space_id"] != space_id or row["generation_id"] != generation_id:
                    raise ValueError("known node belongs to another space or generation")
                nodes[node_id] = self.memory._node_from_row(row)

            for node_id in node_ids:
                await add(node_id)
            parents = set()
            for node_id in node_ids:
                cursor = await connection.execute(
                    "SELECT e.source_node_id FROM memory_edges e JOIN memory_nodes n ON n.node_id=e.source_node_id "
                    "WHERE e.target_node_id=? AND e.relation_type='contains' AND n.node_type='topic' "
                    "AND n.space_id=? AND n.generation_id=? AND n.status='active' "
                    "AND COALESCE(json_extract(n.metadata_json,'$.summary_stale'),0)=0",
                    (node_id, space_id, generation_id))
                parents.update(row["source_node_id"] for row in await cursor.fetchall())
            for node_id in sorted(parents):
                await add(node_id)
            for node in list(nodes.values()):
                if node["node_type"] != "topic":
                    continue
                cursor = await connection.execute(
                    "SELECT source_node_id,target_node_id,relation_type FROM memory_edges "
                    "WHERE source_node_id=? AND relation_type='contains' ORDER BY target_node_id",
                    (node["node_id"],))
                for row in await cursor.fetchall():
                    edges.append(dict(row))
                    await add(row["target_node_id"])
            for node in nodes.values():
                if node["node_type"] != "event":
                    continue
                cursor = await connection.execute(
                    "SELECT source_node_id,target_node_id,relation_type FROM memory_edges "
                    "WHERE source_node_id=? AND relation_type='contains' ORDER BY target_node_id", (node["node_id"],))
                edges.extend(dict(row) for row in await cursor.fetchall() if row["target_node_id"] in nodes)
        edges.sort(key=lambda edge: (edge["source_node_id"], edge["target_node_id"]))
        return list(nodes.values()), edges

    async def _generation(self, **kwargs):
        # Local import avoids the storage module's dependency on digest() at
        # module initialization; the same guard is reused at final commit.
        try:
            from ..storage.extraction_store import assert_extraction_scope
        except ImportError:
            from storage.extraction_store import assert_extraction_scope
        async with self.memory.database.transaction() as connection:
            return dict(await assert_extraction_scope(connection, **kwargs))

    async def flush_image_evidence(self, batch_id: str | None = None) -> None:
        for pending in await self.memory.pending_image_evidence(batch_id):
            await self.sources.save_image_evidence(pending["evidence"])
            await self.memory.acknowledge_image_evidence(pending["batch_id"])

    async def apply(
        self, batch: PreparedExtraction, raw: str, *, model_id: str, image_evidence=()
    ) -> dict[str, Any]:
        output = parse_extraction_output(raw)
        # Check authoritative sources again before entering the memory transaction.
        current = await self.sources.get_records(
            tuple(item["source_id"] for item in batch.sources)
        )
        if digest(current) != digest(batch.sources):
            raise ValueError("source snapshot changed; prepare a new extraction")
        receipt = await self.memory.apply_extraction(
            batch, output, model_id=model_id, prompt_version=batch.prompt_version,
            image_evidence=image_evidence,
        )
        if image_evidence:
            await self.flush_image_evidence(batch.batch_id)
        return receipt

    async def run(
        self,
        batch: PreparedExtraction,
        model: Callable[[list[dict[str, str]]], Awaitable[str]],
        *,
        model_id: str,
        max_prompt_bytes: int = 256_000,
    ) -> dict[str, Any]:
        await self._generation(
            space_id=batch.space["space_id"], generation_id=batch.generation_id,
            job_id=batch.job_id, lease_token=batch.lease_token, allow_building=batch.allow_building,
        )
        completed = await self.memory.get_extraction_batch(batch.batch_id)
        if completed:
            if (
                completed["input_digest"] != batch.input_digest
                or completed["space_id"] != batch.space["space_id"]
                or completed["generation_id"] != batch.generation_id
            ):
                raise ValueError(
                    "completed batch has different input; use its saved receipt"
                )
            await self.flush_image_evidence(batch.batch_id)
            return {**completed, "replayed": True}
        messages = batch.messages()
        if type(max_prompt_bytes) is not int or max_prompt_bytes < 1:
            raise ValueError("max_prompt_bytes must be a positive integer")
        if (
            sum(len(message["content"].encode("utf-8")) for message in messages)
            > max_prompt_bytes
        ):
            raise ValueError(
                "complete extraction prompt exceeds budget; reduce context before retry"
            )
        raw = await model(messages)
        return await self.apply(batch, raw, model_id=model_id)
