"""Low-frequency, resumable synthesis of existing memories into topics."""

from __future__ import annotations

import asyncio
import json
import time

try:
    from ..storage.topic_store import TopicStore
    from ..storage.topic_rebuild_stage import export_topics, persist_topic_stage, topic_rebuild_stage
    from ..storage.topic_rebuild_publish import publish_topics, published_topics, topic_rebuild_basis
except ImportError:
    from storage.topic_store import TopicStore
    from storage.topic_rebuild_stage import export_topics, persist_topic_stage, topic_rebuild_stage
    from storage.topic_rebuild_publish import publish_topics, published_topics, topic_rebuild_basis

from .extraction_apply import digest
from .extraction_context import node_vectors, _rank_page_with_query_coverage
from .jobs import ReviewRequired, deadline_failure, provider_failure
from .model_routing import fixed_route, topic_model_policy
from .attempts import observe_action, observe_commit, observed_call
from .topic_prompt import (PROMPT_VERSION,
                           build_messages, schema_for_prompt_version)
from .topic_schema import MAX_OUTPUT_CHARS, TOPIC_CREATE_MIN_MEMBERS, VERSION, model_output_schema, parse_output_for_context
from .candidate_feedback import compact_json_messages, correction_allowed, recovery_messages, validation_errors
from .topic_regeneration import EXPLANATIONS, VERSION as RECOVERY_VERSION, refresh_feedback
from .topic_evidence import collect_topic_evidence
from .topic_budget import fit_topic_evidence, message_bytes


TOPIC_CONTEXT_ERRORS = {"topic context changed", "topic membership changed", "topic member structure changed", "topic generation is unavailable"}


class TopicMaintenanceMixin:
    topic_prompt_version = PROMPT_VERSION

    async def _topic_blockers(self, space_id, generation_id):
        rows = await self.memory.database.fetchall(
            "SELECT job_id,kind,error_type,error_code,created_at FROM memory_jobs "
            "WHERE scope_key=? AND kind='topic_consolidate' AND state='failed' "
            "AND COALESCE(json_extract(payload_json,'$.topic_inputs_replaced'),0)=0 "
            "AND json_extract(payload_json,'$.expected_generation_id')=? ORDER BY created_at,job_id",
            (space_id, generation_id))
        return [dict(row) for row in rows]

    async def _maybe_enqueue_topics(self, space, generation):
        if not self.config.topic_consolidation_enabled:
            return False
        if await self._topic_blockers(space["space_id"], generation["generation_id"]):
            return False
        multiplier = 2 if space["space_type"] == "group" else 1
        threshold = self.config.topic_min_changes * multiplier
        seeds = await TopicStore(self.memory).dirty_nodes(
            space["space_id"], generation["generation_id"], limit=threshold)
        if not seeds or (len(seeds) < threshold and
                time.time() - seeds[0]["updated_at"] < self.config.topic_idle_seconds * multiplier):
            return False
        key = "topics:auto:" + digest([generation["generation_id"],
            [(node["node_id"], node["content_version"]) for node in seeds]])
        # The same body versions can need review again after an explicit unlock.
        key = await self.jobs.next_window_key(key, reviewed_again=True)
        if key is None:
            return False
        await self.jobs.submit("topic_consolidate", space["space_id"], {
            "space_id": space["space_id"], "expected_generation_id": generation["generation_id"],
            "before": time.time(), "request": {"automatic": True}}, key,
            max_attempts=self.config.job_max_attempts, hide_on_success=True)
        return True

    async def _topic_context(self, space, generation_id, seeds, deadline):
        limit = self.config.topic_context_nodes
        ids = [node["node_id"] for node in seeds]
        embeddings = self.runtime.recall.embeddings if self.runtime.recall is not None else None
        if embeddings is None:
            page = await self.memory.extraction_context_page(space["space_id"], generation_id,
                                                              limit=limit, recent=True, prefer_writable=True)
            return list(dict.fromkeys(ids + [node["node_id"] for node in page["nodes"]]))[:limit]
        seed_ids = set(ids)
        queries, _ = await node_vectors(embeddings, seeds, space["space_id"], deadline=deadline)
        writable, readonly = ([], {}), ([], {})
        after = ""
        fingerprint = embeddings.fingerprint
        while True:
            page = await self.memory.extraction_context_page(space["space_id"], generation_id,
                                                              after=after, limit=128)
            neighbors = [node for node in page["nodes"] if node["node_id"] not in seed_ids]
            if neighbors:
                vectors, _ = await node_vectors(embeddings, neighbors, space["space_id"], deadline=deadline)
                for protected, (best, covered) in ((False, writable), (True, readonly)):
                    indices = [i for i, node in enumerate(neighbors) if bool(node["manual_protected"]) == protected]
                    if indices:
                        await asyncio.to_thread(_rank_page_with_query_coverage, best, covered,
                            [neighbors[i] for i in indices], [vectors[i] for i in indices], queries, limit)
            if embeddings.fingerprint != fingerprint:
                raise ReviewRequired("configuration_changed")
            if not page["has_more"]:
                break
            after = page["after"]
        for best, covered in (writable, readonly):
            for _, node_id in sorted(covered.values(), reverse=True) + best:
                if node_id not in ids and len(ids) < limit:
                    ids.append(node_id)
        return ids

    async def _topic_index(self, job, checkpoint, receipt):
        if not receipt["changed_node_ids"]:
            return
        window = checkpoint["topic_window"]
        required = window.get("index_required") or job["step"].split(":")[0] == "topic_index"
        if not (required or self._index_configured()):
            return
        window["index_required"] = True
        await self._save(job, checkpoint, "topic_index", checkpoint["done"], checkpoint["total"])
        if self.runtime.recall is None:
            raise ReviewRequired("index_provider_required")
        nodes = [await self.memory.get_node(node_id) for node_id in receipt["changed_node_ids"]]
        try:
            await node_vectors(self.runtime.recall.embeddings, nodes, job["scope_key"],
                deadline=time.monotonic() + self.config.index_timeout_seconds, require_persisted=True)
        except Exception as exc:
            raise self._embedding_failure(exc) from None

    async def _prepare_topic_window(self, store, space, generation_id, seeds, *, evidence_memory=None, optional_limit=None):
        """Shrink optional roots and then seed batches without truncating member closures."""
        deadline = time.monotonic() + self.config.extraction_timeout_seconds
        persona = await self._persona_text(space)
        reserve = min(8192, self.config.extraction_max_prompt_bytes // 10)
        usable = self.config.extraction_max_prompt_bytes - reserve
        original_seed_count = len(seeds)
        seed_batch = list(seeds)

        async def fit(seed_ids, optional):
            prepared = await store.prepare(space["space_id"], generation_id, seed_ids + optional, seed_ids=seed_ids)
            options = {"related_topics": prepared["related_topics"], "allowed_user_ids": prepared["allowed_user_ids"],
                       "prompt_version": self.topic_prompt_version}
            nodes, edges = prepared["nodes"], prepared["edges"]
            base = message_bytes(build_messages(persona, nodes, edges, **options))
            diagnostics = {"base_prompt_bytes": base}
            fitted = None
            if not store.can_propose_changes(prepared):
                fitted = []
            elif base <= usable:
                evidence = await collect_topic_evidence(evidence_memory or self.memory, self.sources, nodes, seed_ids)
                fitted, diagnostics = await asyncio.to_thread(
                    fit_topic_evidence, persona, nodes, edges, evidence, usable, **options)
            diagnostics.update(reserved_feedback_bytes=reserve, max_prompt_bytes=self.config.extraction_max_prompt_bytes,
                               original_seed_count=original_seed_count, seed_count=len(seed_ids),
                               optional_roots_retained=len(optional), expanded_nodes=len(nodes),
                               related_topics=len(prepared["related_topics"]))
            if fitted is None:
                return None, diagnostics
            prepared["prompt_version"] = self.topic_prompt_version
            prepared["evidence"] = fitted
            return prepared, diagnostics

        while seed_batch:
            seed_ids = [node["node_id"] for node in seed_batch]
            candidates = await self._topic_context(space, generation_id, seed_batch, deadline)
            optional = [node_id for node_id in candidates if node_id not in seed_ids][:optional_limit]
            prepared, diagnostics = await fit(seed_ids, optional)
            if prepared is None and optional:
                observe_action("resize")
                prepared, diagnostics = await fit(seed_ids, [])
                if prepared is not None:
                    # An optional parent can expand to many members. Try later roots even if it cannot fit.
                    retained = []
                    for node_id in optional:
                        trial, trial_diagnostics = await fit(seed_ids, retained + [node_id])
                        if trial is not None:
                            retained.append(node_id)
                            prepared, diagnostics = trial, trial_diagnostics
            if prepared is not None:
                prepared["input_digest"] = digest({key: value for key, value in prepared.items() if key != "input_digest"})
                return prepared, persona, diagnostics
            if len(seed_batch) == 1:
                failure = ReviewRequired("prompt_budget_exceeded")
                failure.budget = diagnostics
                raise failure
            observe_action("resize")
            seed_batch = seed_batch[:max(1, len(seed_batch) // 2)]

    async def _topic_window(self, job, checkpoint, store, space, generation_id):
        window = checkpoint["topic_window"]
        receipt = await store.receipt(window["receipt_key"])
        if receipt is not None:
            return receipt
        route = window.get("model_route", {"provider_id": self.config.llm_provider_id})
        if store.can_propose_changes(window["prepared"]) and not self.provider_available(route.get("provider_id")):
            raise ReviewRequired("model_required")
        persona, signature = await self._execution(space, route)
        if signature != window["signature"]:
            raise ReviewRequired("configuration_changed")
        if not store.can_propose_changes(window["prepared"]):
            try:
                result = await store.apply(job, window["prepared"], {"schema_version": VERSION, "changes": []},
                                         receipt_key=window["receipt_key"])
                if not result["replayed"]:
                    observe_commit()
                return result
            except ValueError as exc:
                if str(exc) in TOPIC_CONTEXT_ERRORS:
                    raise ReviewRequired("context_changed") from None
                raise
        if window["prepared"]["prompt_version"] != self.topic_prompt_version:
            raise ReviewRequired("configuration_changed")
        messages = build_messages(persona, window["prepared"]["nodes"], window["prepared"]["edges"],
                                  evidence=window["prepared"].get("evidence", ()),
                                  related_topics=window["prepared"].get("related_topics", ()),
                                  allowed_user_ids=window["prepared"].get("allowed_user_ids"),
                                  prompt_version=window["prepared"]["prompt_version"])
        if digest(messages) != window["request_digest"]:
            raise ReviewRequired("configuration_changed")
        schema_version = schema_for_prompt_version(window["prepared"]["prompt_version"])
        deadline = time.monotonic() + self.config.extraction_timeout_seconds
        async with asyncio.timeout_at(deadline) as window_timeout:
            while True:
                if "raw" not in window:
                    request = list(messages)
                    if window.get("regeneration"):
                        observe_action("regenerate")
                        archived = await self.jobs.get_candidate(job["job_id"], window["regeneration"]["candidate_id"])
                        if archived["window"]["prepared"]["input_digest"] != window["prepared"]["input_digest"]:
                            raise ReviewRequired("context_changed")
                        if window["regeneration"].pop("refresh_feedback", False):
                            window["errors"] = refresh_feedback(archived["window"]["raw"],
                                archived["window"].get("errors", []), schema_version=schema_version,
                                context=json.loads(messages[1]["content"]))
                        request = recovery_messages(messages, archived["window"]["raw"], window.get("errors", []), window["regeneration"]["version"])
                    elif window.get("errors"):
                        request += [{"role": "user", "content": "上次候选未通过校验，请依据相同输入重新生成完整候选。错误："
                                    + json.dumps(window["errors"], ensure_ascii=False)}]
                    original_bytes = message_bytes(request)
                    if window.get("regeneration") and original_bytes > self.config.extraction_max_prompt_bytes:
                        request = compact_json_messages(request)
                        window["request_transport"] = {"format": "json-whitespace.v1",
                            "original_bytes": original_bytes, "transport_bytes": message_bytes(request),
                            "canonical_base_digest": window["request_digest"], "transport_digest": digest(request)}
                        await self._save(job, checkpoint, "topic_prepare", checkpoint["done"], checkpoint["total"])
                    else:
                        window.pop("request_transport", None)
                    transport_bytes = message_bytes(request)
                    if transport_bytes > self.config.extraction_max_prompt_bytes:
                        window["request_budget"] = {"prompt_bytes": transport_bytes,
                                                    "max_prompt_bytes": self.config.extraction_max_prompt_bytes}
                        await self._save(job, checkpoint, "topic_prepare", checkpoint["done"], checkpoint["total"])
                        raise ReviewRequired("prompt_budget_exceeded")
                    window.pop("request_budget", None)
                    window.pop("response_diagnostics", None)
                    await self._save(job, checkpoint, "topic_model", checkpoint["done"], checkpoint["total"])
                    options = {}
                    if self.config.extraction_output_format != "text":
                        options = {"output_format": self.config.extraction_output_format,
                                   "output_schema": model_output_schema(schema_version)}
                    diagnostics = {}
                    try:
                        window["raw"] = await observed_call(self.runtime.adapter.generate, request,
                            provider_id=route["provider_id"],
                            **({"model": route["model_override"]} if route.get("model_override") else {}),
                            timeout_seconds=max(.001, deadline - time.monotonic()),
                            diagnostics=diagnostics, **options)
                    except asyncio.CancelledError:
                        if window_timeout.expired():
                            window["response_diagnostics"] = diagnostics
                            await self._save(job, checkpoint, "topic_model", checkpoint["done"], checkpoint["total"])
                        raise
                    except Exception as exc:
                        window["response_diagnostics"] = diagnostics
                        await self._save(job, checkpoint, "topic_model", checkpoint["done"], checkpoint["total"])
                        raise provider_failure(exc) from None
                    window["response_diagnostics"] = diagnostics
                raw = window["raw"]
                if isinstance(raw, str) and len(raw) > MAX_OUTPUT_CHARS:
                    window.pop("raw")
                    window["errors"] = [{"path": "$", "type": "output_capacity_exceeded",
                                         "actual_chars": len(raw), "limit_chars": MAX_OUTPUT_CHARS}]
                    await self._save(job, checkpoint, "topic_validate", checkpoint["done"], checkpoint["total"])
                    raise ReviewRequired("output_budget_exceeded")
                await self._save(job, checkpoint, "topic_validate", checkpoint["done"], checkpoint["total"])
                try:
                    diagnostics = window.get("response_diagnostics", {})
                    if diagnostics.get("finish_reason") == "length" or diagnostics.get("response_status") == "incomplete":
                        window["errors"] = [{"path": "$", "type": "output_incomplete"}]
                        await self._save(job, checkpoint, "topic_validate", checkpoint["done"], checkpoint["total"])
                        raise ReviewRequired("invalid_topic_output")
                    _, current_signature = await self._execution(space, route)
                    if current_signature != signature:
                        raise ReviewRequired("configuration_changed")
                    prepared = window["prepared"]
                    output = parse_output_for_context(window["raw"], prepared,
                        expected_schema_version=schema_version,
                        window_user_ids=prepared["allowed_user_ids"],
                        validate_supplied_versions=False)
                    result = await store.apply(job, window["prepared"], output, receipt_key=window["receipt_key"])
                    if not result["replayed"]:
                        observe_commit()
                    return result
                except PermissionError:
                    raise ReviewRequired("task_write_not_permitted") from None
                except ValueError as exc:
                    if str(exc) in TOPIC_CONTEXT_ERRORS:
                        raise ReviewRequired("context_changed") from None
                    errors = validation_errors(exc, window["raw"], model_output_schema(schema_version), EXPLANATIONS,
                        context=json.loads(messages[1]["content"]), pipeline="topic")
                    window["errors"] = errors
                    await self._save(job, checkpoint, "topic_validate", checkpoint["done"], checkpoint["total"])
                    if (not self.config.extraction_auto_regeneration
                            or not correction_allowed(errors, EXPLANATIONS)
                            or window["corrections"] >= window["correction_limit"]):
                        raise ReviewRequired("invalid_topic_output") from None
                    candidate_id = await self.jobs.archive_candidate(job["job_id"], job["lease_token"],
                                                                    window, "invalid_topic_output")
                    window["corrections"] += 1
                    window.pop("raw")
                    window["regeneration"] = {"version": RECOVERY_VERSION, "candidate_id": candidate_id}
                    await self._save(job, checkpoint, "correcting", checkpoint["done"], checkpoint["total"])

    async def _consolidate_topics(self, job, *, stage_for=None):
        store = TopicStore(self.memory)
        payload = job["payload"]
        if payload.get("topic_inputs_replaced"):
            raise ReviewRequired("context_changed")
        space = await self.memory.get_space(job["scope_key"])
        if job["kind"] == "rebuild":
            generation = {"generation_id": payload["topic_generation_id"]}
        else:
            generation = await self.memory.get_active_generation(space["space_id"])
            if generation["generation_id"] != payload["expected_generation_id"]:
                raise ReviewRequired("context_changed")
        checkpoint = dict(job["checkpoint"])
        checkpoint.setdefault("done", 0)
        checkpoint.setdefault("topics", [])
        if "total" not in checkpoint:
            checkpoint["total"] = await store.count_dirty(space["space_id"], generation["generation_id"],
                                                         before=payload.get("before"), review_before=payload.get("review_before"))
        while True:
            if "topic_window" not in checkpoint:
                shrink = checkpoint.get("topic_shrink", {})
                seeds = await store.dirty_nodes(space["space_id"], generation["generation_id"],
                                                limit=shrink.get("seed_limit", 8), before=payload.get("before"), review_before=payload.get("review_before"))
                if not seeds:
                    break
                await self._save(job, checkpoint, "topic_prepare", checkpoint["done"], checkpoint["total"])
                try:
                    async with asyncio.timeout(self.config.extraction_timeout_seconds):
                        prepared, persona, budget = await self._prepare_topic_window(
                            store, space, generation["generation_id"], seeds, evidence_memory=stage_for,
                            optional_limit=shrink.get("optional_limit"))
                except TimeoutError as exc:
                    raise deadline_failure(exc, "context_embedding_timeout") from None
                except ReviewRequired as exc:
                    if hasattr(exc, "budget"):
                        checkpoint["topic_budget"] = exc.budget
                        await self._save(job, checkpoint, "topic_prepare", checkpoint["done"], checkpoint["total"])
                    raise
                except Exception as exc:
                    raise self._embedding_failure(exc, context=True) from None
                checkpoint["topic_budget"] = budget
                if shrink and not store.can_propose_changes(prepared):
                    raise ReviewRequired("invalid_topic_output")
                policy = payload.get("model_policy")
                if policy is None:
                    policy = topic_model_policy(await (stage_for or self.memory).get_space_policy(space["space_id"]),
                                                self.config, payload.get("request", {}).get("model_selection", {}))
                route = fixed_route(policy, policy.get("default_provider_id", self.config.llm_provider_id),
                                    payload.get("request", {}).get("model_selection", {}), topic=True)
                if not route and store.can_propose_changes(prepared):
                    routes = await self.topic_routes(stage_for or self.memory, space, generation["generation_id"],
                        [n["node_id"] for n in prepared["nodes"] if n["node_type"] != "topic"])
                    if len(routes) == 1:
                        route = routes[0]
                if store.can_propose_changes(prepared) and not self.provider_available(route.get("provider_id")):
                    raise ReviewRequired("model_required")
                _, signature = await self._execution(space, route)
                checkpoint["topic_window"] = {"prepared": prepared, "signature": signature, "model_route": route,
                    "budget": budget, "index_required": self._index_configured(),
                    "request_digest": digest(build_messages(persona, prepared["nodes"], prepared["edges"],
                        evidence=prepared["evidence"], related_topics=prepared["related_topics"], allowed_user_ids=prepared["allowed_user_ids"], prompt_version=self.topic_prompt_version)),
                    "receipt_key": digest([job["job_id"], prepared["input_digest"], VERSION]),
                    "corrections": 0, "correction_limit": self.config.extraction_max_regenerations}
                await self._save(job, checkpoint, "topic_prepare", checkpoint["done"], checkpoint["total"])
            try:
                receipt = await self._topic_window(job, checkpoint, store, space, generation["generation_id"])
            except TimeoutError as exc:
                raise deadline_failure(exc, "topic_timeout") from None
            except ReviewRequired as exc:
                window = checkpoint["topic_window"]
                diagnostics = window.get("response_diagnostics", {})
                capacity = any(error.get("type") == "output_capacity_exceeded" for error in window.get("errors", []))
                truncated = diagnostics.get("finish_reason") == "length" or diagnostics.get("incomplete_reason") == "max_output_tokens"
                if exc.code not in {"invalid_topic_output", "output_budget_exceeded"} or not (truncated or capacity):
                    raise
                seed_count = len(window["prepared"]["seed_ids"])
                optional_count = window.get("budget", {}).get("optional_roots_retained", 0)
                required_neighbors = max(0, TOPIC_CREATE_MIN_MEMBERS - seed_count)
                if optional_count > required_neighbors:
                    optional_limit = max(required_neighbors, optional_count // 2)
                elif seed_count > TOPIC_CREATE_MIN_MEMBERS:
                    seed_count = max(TOPIC_CREATE_MIN_MEMBERS, seed_count // 2)
                    optional_limit = 0
                else:
                    raise
                if "raw" in window:
                    await self.jobs.archive_candidate(job["job_id"], job["lease_token"], window, exc.code)
                observe_action("resize")
                checkpoint["topic_shrink"] = {"seed_limit": seed_count, "optional_limit": optional_limit}
                checkpoint.pop("topic_window")
                await self._save(job, checkpoint, "topic_prepare", checkpoint["done"], checkpoint["total"])
                continue
            checkpoint.pop("topic_shrink", None)
            version = checkpoint["topic_window"]["prepared"]["prompt_version"]
            checkpoint["prompt_versions"] = sorted(set(checkpoint.get("prompt_versions", [])) | {version})
            if stage_for is None:
                await self._topic_index(job, checkpoint, receipt)
            else:
                await persist_topic_stage(stage_for, self.memory, job, checkpoint["topic_window"], receipt)
            checkpoint["done"] += receipt["reviewed_count"]
            checkpoint["topics"] = list(dict.fromkeys(checkpoint["topics"] + receipt["topic_ids"]))
            checkpoint.pop("topic_window")
            await self._save(job, checkpoint, "topic_prepare", checkpoint["done"], checkpoint["total"])
        versions = checkpoint.get("prompt_versions", [])
        return {"reviewed_count": checkpoint["done"], "topic_ids": checkpoint["topics"],
                "generation_id": generation["generation_id"], "protocol": VERSION,
                "prompt_version": versions[0] if len(versions) == 1 else None,
                "prompt_versions": versions}

    async def _replace_topics(self, job):
        space_id = job["scope_key"]
        generation_id = job["payload"]["expected_generation_id"]
        result = await published_topics(self.memory, job["job_id"])
        if result is None:
            generation = await self.memory.get_active_generation(space_id)
            if not generation or generation["generation_id"] != generation_id:
                raise ReviewRequired("context_changed")
            basis = await topic_rebuild_basis(self.memory, space_id, generation_id)
            checkpoint = dict(job["checkpoint"])
            index_required = self._index_configured() or checkpoint.get("index_required", False)
            if "index_required" not in checkpoint and job["step"] == "topic_index":
                index_required |= any(attempt.get("error_code", "").startswith(("index_", "window_index_"))
                                      for attempt in job["attempts"] if attempt.get("error_code"))
            if "basis" not in checkpoint:
                checkpoint["basis"] = basis
                checkpoint["index_required"] = index_required
                await self._save(job, checkpoint, "topic_prepare")
            if checkpoint["basis"] != basis:
                raise ReviewRequired("context_changed")
            async with topic_rebuild_stage(self.memory, space_id, generation_id, job) as stage:
                worker = self.snapshot()
                worker.memory = stage
                synthesis = await worker._consolidate_topics({**job, "checkpoint": checkpoint}, stage_for=self.memory)
                drafts, reviewed = await export_topics(stage)
            done = len(reviewed)
            checkpoint = {"basis": basis, "done": done, "total": done, "index_required": index_required,
                          "prompt_versions": synthesis["prompt_versions"]}
            await self._save(job, checkpoint, "topic_index", done, done)
            if index_required and drafts:
                if self.runtime.recall is None:
                    raise ReviewRequired("index_provider_required")
                nodes = [{"node_id": draft["node_id"], "content_version": 1, "content_text": draft["content"]}
                         for draft in drafts]
                try:
                    await node_vectors(self.runtime.recall.embeddings, nodes, space_id,
                        deadline=time.monotonic() + self.config.index_timeout_seconds, require_persisted=True)
                except Exception as exc:
                    raise self._embedding_failure(exc) from None
            await self._save(job, checkpoint, "topic_publish", done, done)
            async def invalidate_nodes(namespace, ids):
                try:
                    await self.runtime.cache.invalidate_nodes(namespace, ids)
                except Exception as exc:
                    raise self._embedding_failure(exc) from None

            try:
                result = await publish_topics(self.memory, job, generation_id, drafts, reviewed, basis,
                    invalidate_nodes=invalidate_nodes if self.runtime.cache is not None else None)
                observe_commit()
            except ValueError as exc:
                if str(exc) == "topic context changed":
                    raise ReviewRequired("context_changed") from None
                raise
        return {"generation_id": generation_id, "reviewed_count": result["reviewed_count"],
                "topic_ids": result["topic_ids"], "removed_count": len(result["removed_node_ids"])}

    async def _rebuild_topics(self, job, checkpoint, space, generation_id):
        """Keep synthesis checkpoints inside the rebuild lease and unpublished generation."""
        checkpoint.setdefault("topic_before", time.time())
        worker = self.snapshot()

        async def save(_job, topic_checkpoint, step, done=0, total=None):
            checkpoint["topic_synthesis"] = topic_checkpoint
            await self._save(job, checkpoint, f"{step}:{done}/{total or 0}",
                             checkpoint["done"], checkpoint["total"])

        worker._save = save
        nested = {**job, "checkpoint": checkpoint.get("topic_synthesis", {}),
                  "payload": {**job["payload"], "topic_generation_id": generation_id,
                              "before": checkpoint["topic_before"]}}
        result = await worker._consolidate_topics(nested)
        checkpoint["topic_result"] = result
        checkpoint.pop("topic_synthesis", None)
        return result
