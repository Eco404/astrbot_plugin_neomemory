"""Choose a callable provider from the frozen conversation or task settings."""
import json
from collections import defaultdict


def chat_route(record):
    if record.get("actor_kind") != "agent":
        return {}
    metadata = record.get("metadata")
    if metadata is None:
        metadata = json.loads(record.get("metadata_json") or "{}")
    return metadata.get("chat_model") or {}


def fixed_route(policy, default, selection, *, topic=False):
    override = selection.get("topic_provider_id" if topic else "window_provider_id")
    if override:
        return {"provider_id": override, "basis": "task"}
    if topic and policy.get("topic_provider_id"):
        return {"provider_id": policy["topic_provider_id"], "basis": "topic"}
    mode = policy.get("model_mode", "inherit")
    if mode == "fixed":
        return {"provider_id": policy["provider_id"], "basis": "space"}
    if mode == "inherit" and default:
        return {"provider_id": default, "basis": "plugin"}
    return {}


def window_route(policy, default, records, selection):
    fixed = fixed_route(policy, default, selection)
    if fixed:
        return fixed
    for record in sorted(records, key=lambda row: (row.get("observed_at", 0), row.get("sequence_no", 0)), reverse=True):
        route = chat_route(record)
        if route.get("provider_id"):
            return route
    fallback = selection.get("missing_window_provider_id")
    return {"provider_id": fallback, "basis": "task_fallback"} if fallback else {}


def validate_selection(selection):
    if not isinstance(selection, dict) or selection.keys() - {
            "window_provider_id", "topic_provider_id", "missing_window_provider_id"}:
        raise ValueError("unknown task model selection")
    if any(not isinstance(value, str) or not value.strip() for value in selection.values()):
        raise ValueError("select a provider for each model override")
    if "window_provider_id" in selection and "missing_window_provider_id" in selection:
        raise ValueError("choose either an override or a fallback for source windows")


class ModelSelectionRequired(ValueError):
    """The requested work needs an explicit provider before it can be queued."""


class ModelRoutingMixin:
    async def live_plan(self, space, generation_id, *, manual=False):
        records = await self._pending(space, generation_id)
        if len({record["stream_id"] for record in records}) <= 1:
            return records, await self._decision(space, records, manual=manual)
        ready = []
        for stream in sorted(await self.scanner.highwater(space)):
            rows = await self.scanner.pending_stream(space, generation_id, stream, self.memory.path)
            decision = await self._decision(space, rows, manual=manual)
            if decision.status != "waiting":
                ready.append((rows, decision))
        if ready:
            return min(ready, key=lambda item: item[0][0]["observed_at"])
        return [], await self._decision(space, [], manual=manual)

    async def replay_plan(self, space, payload, offsets):
        """Select only whole single-stream windows, preserving pending tails for live use."""
        ready = []
        for stream, high in payload["highwater"].items():
            page = await self.scanner.replay_page(space, {stream: high}, offsets.get(stream, 0))
            if not page.records:
                continue
            decision = await self._decision(space, page.records, replay=True)
            if decision.status != "waiting":
                ready.append((page.records[0]["observed_at"], stream, page.records, decision))
        if not ready:
            return None
        _, stream, records, decision = min(ready, key=lambda item: item[:2])
        return stream, records, decision

    def provider_available(self, provider_id):
        if not provider_id:
            return False
        try:
            self.runtime.adapter.llm_fingerprint(provider_id, self.config.extraction_model_revision)
        except (ValueError, LookupError, TypeError):
            return False
        return True

    def resolve_window_route(self, policy, records, selection, default=None):
        route = window_route(policy, self.config.llm_provider_id if default is None else default, records, selection)
        if self.provider_available(route.get("provider_id")):
            return route
        fallback = selection.get("missing_window_provider_id")
        if self.provider_available(fallback):
            return {"provider_id": fallback, "basis": "task_fallback"}
        return {}

    async def topic_routes(self, memory, space, generation_id, node_ids=None):
        params = [space["space_id"], generation_id]
        where = "n.space_id=? AND n.generation_id=? AND n.node_type<>'topic' AND n.status='active'"
        if node_ids is not None:
            where += " AND n.node_id IN (SELECT value FROM json_each(?))"
            params.append(json.dumps(node_ids))
        # Include the extraction window that established the member, since the
        # member's direct citations can legitimately include only user messages.
        rows = await memory.database.fetchall(f"""
            WITH nodes AS (SELECT node_id FROM memory_nodes n WHERE {where}),
            refs AS (SELECT s.node_id,s.source_id FROM nodes JOIN node_sources s USING(node_id)
              UNION SELECT v.node_id,b.source_id FROM nodes JOIN memory_node_versions v USING(node_id)
              JOIN memory_derivations d ON d.node_version_id=v.node_version_id
              JOIN extraction_batch_sources b ON b.batch_id=d.batch_id)
            SELECT nodes.node_id,refs.source_id FROM nodes LEFT JOIN refs USING(node_id)""", tuple(params))
        members = defaultdict(set)
        for row in rows:
            members[row["node_id"]].add(row["source_id"])
        ids = list({source_id for refs in members.values() for source_id in refs if source_id})
        by_source = {}
        for start in range(0, len(ids), 200):
            records = await self.sources.database.fetchall(
                "SELECT source_id,actor_kind,metadata_json FROM source_records WHERE source_id IN (SELECT value FROM json_each(?))",
                (json.dumps(ids[start:start + 200]),))
            for row in records:
                route = chat_route(dict(row))
                if route.get("provider_id"):
                    by_source[row["source_id"]] = route
        routes = {}
        for refs in members.values():
            found = [by_source[source_id] for source_id in refs if source_id in by_source]
            for route in found or [{}]:
                routes[(route.get("provider_id"), route.get("model_override"))] = route
        return list(routes.values())

    async def _preview_windows(self, kind, space, generation_id, highwater):
        if kind == "extract":
            records, decision = await self.live_plan(space, generation_id, manual=True)
            selected = set(decision.source_ids or (decision.blocked_source_id,))
            if records:
                yield [row for row in records if row["source_id"] in selected]
            return
        for stream, high in highwater.items():
            offset = 0
            while True:
                page = await self.scanner.replay_page(space, {stream: high}, offset)
                decision = await self._decision(space, page.records, replay=True)
                if decision.status != "ready":
                    break
                selected = set(decision.source_ids)
                yield [row for row in page.records if row["source_id"] in selected]
                offset += len(selected)

    def _route_summary(self, routes, *, windows=False):
        valid = [route for route in routes if self.provider_available(route.get("provider_id"))]
        providers = sorted({route["provider_id"] for route in valid})
        distinct = {(route["provider_id"], route.get("model_override")) for route in valid}
        missing = len(routes) - len(valid)
        mode = "missing"
        if valid and not missing:
            if len(distinct) == 1:
                mode = "fixed"
            elif windows:
                mode = "windows"
        bases = {route.get("basis", "chat") for route in valid}
        return {"mode": mode, "provider_id": providers[0] if mode == "fixed" else "",
                "providers": providers, "missing_windows": missing,
                "basis": next(iter(bases)) if len(bases) == 1 else "chat"}

    async def model_preview(self, kind, space, *, include_topics=False, selection=None, policy=None, highwater=None):
        selection = selection or {}
        policy = policy if policy is not None else await self.memory.get_space_policy(space["space_id"])
        default = self.config.llm_provider_id
        generation = await self.memory.get_active_generation(space["space_id"])
        generation_id = generation["generation_id"]
        window_step = kind in {"extract", "rebuild", "profile_rebuild", "relationship_rebuild", "context_rebuild"}
        topic_step = include_topics or kind in {"topic_consolidate", "topic_rebuild"} or (
            kind == "rebuild" and self.config.topic_consolidation_enabled)
        window_fixed = fixed_route(policy, default, selection)
        topic_fixed = fixed_route(policy, default, selection, topic=True)
        window_routes, topic_routes = [], []
        if (window_step and not window_fixed) or (kind == "rebuild" and topic_step and not topic_fixed):
            if kind != "extract" and highwater is None:
                highwater = await self.scanner.highwater(space)
            async for records in self._preview_windows(kind, space, generation_id, highwater):
                window_routes.append(self.resolve_window_route(policy, records, selection))
                if kind == "rebuild" and topic_step and not topic_fixed:
                    observed = [chat_route(row) for row in records]
                    topic_routes.extend([route for route in observed if route.get("provider_id")] or [{}])
        result = {}
        if window_step:
            result["window"] = self._route_summary([window_fixed] if window_fixed else window_routes, windows=True)
        if topic_step:
            if topic_fixed:
                topic_routes = [topic_fixed]
            elif kind != "rebuild":
                topic_routes = await self.topic_routes(self.memory, space, generation_id)
            result["topic"] = self._route_summary(topic_routes)
        return result
