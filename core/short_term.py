"""Bounded, durable rolling summaries; never wait for an LLM in a chat hook."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import inspect
import json
import time
from typing import Any

try:
    from ..storage.job_store import JobConflict
    from ..storage.short_term_store import ShortTermStore
    from ..storage.source_scan import SourceScanner
except ImportError:
    from storage.job_store import JobConflict
    from storage.short_term_store import ShortTermStore
    from storage.source_scan import SourceScanner

from .jobs import ModelNetworkError, RetryableJobError, ReviewRequired
from .model_routing import validate_selection, window_route
from .provider_errors import is_transient_provider_error
from .persona_policy import is_supported_persona
from .memory_text import user_reference_ids
from .forwarded_content import window_text
from .group_addressing import address_snapshot_users
from .short_term_prompts import (MAX_OUTPUT_BYTES, PROMPT_VERSION, host_has_summary, messages,
                                 output_schema, parse_output, provider_schema)
from .candidate_feedback import (
    SHORT_TERM_RECOVERY_VERSION as RECOVERY_VERSION, correction_allowed, recovery_messages, validation_errors,
)
RECOVERY_EXPLANATIONS = {
    "summary_unknown_user": "用户引用只能来自known_user_ids、新来源的logical_user_id或已有摘要身份，不编造用户、不换人。",
    "initial_summary_evidence_required": "没有旧摘要时，非空新摘要至少需要一条本窗口来源；只有寒暄可返回空摘要，不能为保留无依据内容机械添加无关来源。",
}


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def conversation_key(stream_id: str, host_conversation_id: str) -> str:
    if not isinstance(stream_id, str) or not stream_id or not isinstance(host_conversation_id, str) or not host_conversation_id:
        raise ValueError("short-term memory needs explicit stream and host conversation IDs")
    return "host.v1:" + _digest([stream_id, host_conversation_id])


class ShortTermService:
    def __init__(self, sources, memory, jobs, adapter, config, persona_loader):
        self.sources, self.memory, self.jobs = sources, memory, jobs
        self.adapter, self.config, self.persona_loader = adapter, config, persona_loader
        self.scanner = SourceScanner(sources)
        self.store = ShortTermStore(memory)

    def _provider_id(self):
        return getattr(self.config, "short_term_provider_id", "") or self.config.llm_provider_id

    async def _route(self, space_id, records, selection=None):
        selection = selection or {}
        dedicated = getattr(self.config, "short_term_provider_id", "")
        if dedicated and not selection.get("window_provider_id"):
            route = {"provider_id": dedicated, "basis": "short_term"}
        else:
            policy = await self.memory.get_space_policy(space_id)
            route = window_route(policy, self.config.llm_provider_id, records, selection)
        if route:
            try:
                self._fingerprint(route["provider_id"])
            except (ValueError, LookupError, TypeError):
                return {}
        return route

    def _fingerprint(self, provider_id):
        return self.adapter.llm_fingerprint(provider_id, getattr(self.config, "extraction_model_revision", ""))

    async def model_preview(self, job):
        records = await self.sources.get_records(tuple(job["payload"]["source_ids"]))
        route = await self._route(job["scope_key"], records)
        return {"window": {"mode": "fixed" if route else "missing",
                           "provider_id": route.get("provider_id", ""),
                           "providers": [route["provider_id"]] if route else [],
                           "missing_windows": 0 if route else 1, "basis": route.get("basis", "")}}

    def _policy(self):
        return {"keep_messages": getattr(self.config, "short_term_keep_messages", 10),
                "min_messages": getattr(self.config, "short_term_min_messages", 6),
                "max_messages": getattr(self.config, "short_term_max_messages", 40),
                "max_chars": getattr(self.config, "short_term_max_chars", 16000),
                "summary_chars": getattr(self.config, "short_term_summary_chars", 400)}

    @staticmethod
    def _source_prefix(records, max_chars):
        """Return the longest complete source prefix within the summary budget."""
        chosen = []
        chars = 0
        for record in records:
            size = len(window_text(record))
            if chosen and chars + size > max_chars:
                break
            chosen.append(record)
            chars += size
        return chosen

    def _query(self, space, *, stream_id, conversation_id):
        query, args = self.scanner._scope_query(space)
        query += ", matching AS (SELECT r.* FROM eligible e JOIN source_records r USING(source_id) " \
                 "WHERE e.replay_only=0 AND r.stream_id=? AND json_extract(r.metadata_json,'$.host_conversation_id')=?) "
        return query, (*args, stream_id, conversation_id)

    async def _stream(self, space, conversation_id):
        query, args = self.scanner._scope_query(space)
        rows = await self.sources.database.fetchall(
            query + "SELECT DISTINCT r.stream_id FROM eligible e JOIN source_records r USING(source_id) "
            "WHERE json_extract(r.metadata_json,'$.host_conversation_id')=? LIMIT 2",
            (*args, conversation_id),
        )
        return rows[0]["stream_id"] if len(rows) == 1 else None

    async def get_current(self, space_id, conversation_id, *, stream_id, host_contexts=None):
        if not conversation_id or not stream_id or host_has_summary(host_contexts):
            return None
        space = await self.memory.get_space(space_id)
        if not space or not is_supported_persona(space["persona_id"]):
            return None
        value = await self.store.get(space_id, conversation_key(stream_id, conversation_id))
        if (not value or value["status"] != "active" or value["coverage"].get("stream_id") != stream_id
                or value["coverage"].get("host_conversation_id") != conversation_id):
            return None
        return value

    async def maybe_enqueue(self, space_id, conversation_id, *, stream_id=None, host_contexts=None):
        return await copy.copy(self)._maybe_enqueue(
            space_id, conversation_id, stream_id=stream_id, host_contexts=host_contexts)

    async def _maybe_enqueue(self, space_id, conversation_id, *, stream_id=None, host_contexts=None, replaces=None):
        if (not getattr(self.config, "short_term_enabled", True)
                or not conversation_id or host_has_summary(host_contexts)):
            return None
        space = await self.memory.get_space(space_id)
        if not space or not is_supported_persona(space["persona_id"]):
            return None
        stream_id = stream_id or await self._stream(space, conversation_id)
        if not stream_id:
            return None
        key = conversation_key(stream_id, conversation_id)
        generation = await self.memory.get_active_generation(space_id)
        if not generation:
            return None
        previous = await self.store.get(space_id, key, generation_id=generation["generation_id"])
        if previous and previous["status"] != "active":
            return None
        # Recovery owns the existing failed window. New
        # messages must not automatically recreate an overlapping cancelled
        # window. A successful replacement advances the summary version, so
        # old cancelled attempts no longer block the following increment.
        stopped = await self.memory.database.fetchone(
            "SELECT job_id FROM memory_jobs WHERE scope_key=? AND kind='short_term' "
            "AND state IN ('failed','cancelled') "
            "AND COALESCE(json_extract(result_json,'$.replacement_job_id'),'')='' "
            "AND json_extract(payload_json,'$.generation_id')=? "
            "AND json_extract(payload_json,'$.conversation_key')=? "
            "AND json_extract(payload_json,'$.previous_version')=? LIMIT 1",
            (space_id, generation["generation_id"], key, previous["content_version"] if previous else 0),
        )
        if stopped or (not replaces and not (await self.jobs.availability(space_id))["available"]):
            return None
        policy = self._policy()
        query, args = self._query(space, stream_id=stream_id, conversation_id=conversation_id)
        # Reserve the newest N sources even when the oldest unfinished range is
        # larger than one batch; selecting the last N from a page would be wrong.
        boundary = await self.sources.database.fetchone(
            query + "SELECT sequence_no FROM matching ORDER BY sequence_no DESC LIMIT 1 OFFSET ?",
            (*args, policy["keep_messages"]),
        )
        if not boundary:
            return None
        lower = previous["coverage"].get("to_sequence", 0) if previous else 0
        candidates = await self.sources.database.fetchall(
            query + "SELECT source_id,content_text,content_json,metadata_json,actor_kind,observed_at,sequence_no FROM matching "
            "WHERE sequence_no>? AND sequence_no<=? ORDER BY sequence_no LIMIT ?",
            (*args, lower, boundary["sequence_no"], policy["max_messages"]),
        )
        if len(candidates) < policy["min_messages"]:
            return None
        chosen, chars = [], 0
        for row in candidates:
            source_chars = len(window_text(dict(row)))
            if chosen and chars + source_chars > policy["max_chars"]:
                break
            chosen.append(dict(row))
            chars += source_chars
            if chars >= policy["max_chars"]:
                break
        # A large first source remains an explicit failed task, never silently
        # truncated or skipped. A size-limited prefix may contain fewer than min.
        payload = {"generation_id": generation["generation_id"], "conversation_key": key,
                   "stream_id": stream_id, "conversation_id": conversation_id,
                   "previous_version": previous["content_version"] if previous else 0,
                   "source_ids": [row["source_id"] for row in chosen], "keep_messages": policy["keep_messages"],
                   "model_route": await self._route(space_id, chosen)}
        request_key = "auto-restart:" + replaces["job_id"] if replaces else "short_term:" + _digest([space_id, payload])
        try:
            return await self.jobs.submit("short_term", space_id, payload, request_key,
                                          max_attempts=getattr(self.config, "job_max_attempts", 2), hide_on_success=True, replaces=replaces)
        except JobConflict:
            if replaces:
                raise
            return None

    async def auto_reprepare(self, job):
        payload = job["payload"]
        return await self._maybe_enqueue(job["scope_key"], payload["conversation_id"],
                                         stream_id=payload["stream_id"], replaces=job)

    async def retry(self, job_id, *, fresh=False, model_selection=None):
        """Explicit resume or reprepare the same frozen source window."""
        job = await self.jobs.get(job_id)
        if not job or job["kind"] != "short_term":
            raise ValueError("unknown short-term task")
        if not fresh:
            if model_selection:
                raise ValueError("choose a model when preparing a fresh task")
            return await self.jobs.retry(job_id)
        if job["state"] not in {"failed", "cancelled"}:
            raise JobConflict("only failed or cancelled summaries can be reprepared")
        generation = await self.memory.get_active_generation(job["scope_key"])
        if not generation or generation["generation_id"] != job["payload"]["generation_id"]:
            raise ValueError("summary generation changed; use the current conversation")
        selection = model_selection or {}
        validate_selection(selection)
        if selection.keys() - {"window_provider_id"}:
            raise ValueError("select a short-term summary provider")
        payload = dict(job["payload"])
        records = await self.sources.get_records(tuple(payload["source_ids"]))
        policy = self._policy()
        if sum(len(window_text(row)) for row in records) > policy["max_chars"]:
            records = self._source_prefix(records, policy["max_chars"])
            payload["source_ids"] = [row["source_id"] for row in records]
            if not records:
                raise ValueError("short-term source window cannot fit its first source")
        payload["model_route"] = await self._route(job["scope_key"], records, selection)
        if not payload["model_route"]:
            raise ValueError("model_required: select a short-term summary model")
        result = await self.jobs.submit("short_term", job["scope_key"], payload,
            "short_term_fresh:" + job_id, max_attempts=getattr(self.config, "job_max_attempts", 2))
        await self.jobs.cancel(job_id)
        return result

    async def _execution(self, space, prompt_version=PROMPT_VERSION, route=None):
        persona = self.persona_loader(space["persona_id"])
        if inspect.isawaitable(persona):
            persona = await persona
        if not isinstance(persona, str) or not persona.strip():
            raise ReviewRequired("persona_unavailable")
        provider = self._provider_id() if route is None else route.get("provider_id")
        if not provider:
            raise ReviewRequired("model_required")
        try:
            model = self._fingerprint(provider)
        except (ValueError, LookupError, TypeError):
            raise ReviewRequired("model_required") from None
        return persona, _digest([prompt_version, persona, model, self._policy()])

    async def _save(self, job, checkpoint, step):
        await self.jobs.save_checkpoint(job["job_id"], job["lease_token"], checkpoint=checkpoint,
                                       step=step, progress_done=0,
                                       progress_total=len(job["payload"]["source_ids"]))

    async def run(self, job):
        timeout = getattr(self.config, "extraction_timeout_seconds", 180)
        try:
            async with asyncio.timeout(timeout):
                return await copy.copy(self)._run(job, deadline=time.monotonic() + timeout)
        except TimeoutError:
            raise RetryableJobError("short_term_timeout") from None

    async def _run(self, job, *, deadline):
        receipt = await self.store.receipt(job["job_id"])
        if receipt:
            return receipt  # A commit followed by a crash must not call the model again.
        payload = job["payload"]
        space = await self.memory.get_space(job["scope_key"])
        if not space:
            raise ReviewRequired("space_missing")
        if not is_supported_persona(space["persona_id"]):
            return {"skipped": True, "reason": "unsupported_persona"}
        generation = await self.memory.get_active_generation(job["scope_key"])
        if not generation or generation["generation_id"] != payload["generation_id"]:
            raise ReviewRequired("generation_changed")
        previous = await self.store.get(job["scope_key"], payload["conversation_key"],
                                        generation_id=payload["generation_id"])
        if (previous["content_version"] if previous else 0) != payload["previous_version"]:
            raise ReviewRequired("summary_changed")
        if previous and previous["status"] != "active":
            raise ReviewRequired("summary_disabled")
        policy = self._policy()
        # Verify the exact eligible prefix still exists. Source records removed
        # between admission and execution must never be silently jumped over.
        query, args = self._query(space, stream_id=payload["stream_id"], conversation_id=payload["conversation_id"])
        rows = await self.sources.database.fetchall(
            query + "SELECT source_id FROM matching WHERE sequence_no>? ORDER BY sequence_no LIMIT ?",
            (*args, previous["coverage"].get("to_sequence", 0) if previous else 0, len(payload["source_ids"])),
        )
        if [row["source_id"] for row in rows] != payload["source_ids"]:
            raise ReviewRequired("source_range_changed")
        records = await self.sources.get_records(tuple(payload["source_ids"]))
        checkpoint = dict(job.get("checkpoint") or {})
        prompt_version = checkpoint["prompt_version"] if checkpoint else PROMPT_VERSION
        source_texts = [window_text(row) for row in records]
        if sum(map(len, source_texts)) > policy["max_chars"]:
            raise ReviewRequired("source_budget_exceeded")
        persona, signature = await self._execution(space, prompt_version, job["payload"].get("model_route"))
        # A forward's quoted words can exist only in the component JSON.
        sources = [(row["source_id"], text)
                   for row, text in zip(records, source_texts)]
        input_digest = _digest([payload, previous["summary_text"] if previous else "", sources, signature])
        if checkpoint and checkpoint.get("input_digest") != input_digest:
            raise ReviewRequired("configuration_or_context_changed")
        if not checkpoint:
            user_ids = {row["actor_logical_user_id"] for row in records if row.get("actor_logical_user_id")}
            user_ids.update(address_snapshot_users(records))
            user_ids.update(user_reference_ids([previous["summary_text"] if previous else ""]))
            if space.get("logical_user_id"):
                user_ids.add(space["logical_user_id"])
            checkpoint = {"input_digest": input_digest, "signature": signature, "prompt_version": prompt_version,
                          "user_ids": sorted(user_ids),
                          "corrections": 0, "correction_limit": getattr(self.config, "extraction_max_regenerations", 3)}
            checkpoint["output_format"] = getattr(self.config, "extraction_output_format", "json_schema")
            await self._save(job, checkpoint, "short_term_prepared")
        old_text = previous["summary_text"] if previous else ""
        prompt = messages(persona=persona, previous_summary=old_text, records=records,
                          summary_chars=policy["summary_chars"], user_ids=checkpoint["user_ids"], prompt_version=prompt_version)
        schema = output_schema(source_ids=payload["source_ids"], summary_chars=policy["summary_chars"],
                               previous_summary=old_text, prompt_version=prompt_version)
        candidate = await self._candidate(job, checkpoint, prompt, schema, space, policy, old_text, deadline)
        try:
            return await self.store.apply(job, input_digest=input_digest,
                summary_text=candidate["summary_text"], records=records,
                evidence_source_ids=candidate["evidence_source_ids"])
        except ValueError:
            raise ReviewRequired("summary_context_changed") from None

    async def _candidate(self, job, checkpoint, prompt, schema, space, policy, previous_summary, deadline):
        prompt_version = checkpoint["prompt_version"]
        while True:
            request = prompt
            if checkpoint.get("regeneration"):
                archived = await self.jobs.get_candidate(job["job_id"], checkpoint["regeneration"]["candidate_id"])
                if archived["window"]["input_digest"] != checkpoint["input_digest"]:
                    raise ReviewRequired("configuration_or_context_changed")
                request = recovery_messages(prompt, archived["window"]["raw"], checkpoint.get("errors", []), checkpoint["regeneration"]["version"])
            if "raw" not in checkpoint:
                await self._generate(job, checkpoint, request, schema, deadline)
            if checkpoint["signature"] != (await self._execution(space, prompt_version, job["payload"].get("model_route")))[1]:
                raise ReviewRequired("configuration_changed")
            diagnostics = checkpoint.get("response_diagnostics", {})
            if diagnostics.get("finish_reason") == "length" or diagnostics.get("response_status") == "incomplete":
                checkpoint["errors"] = [{"path": "$", "type": "output_incomplete"}]
                await self._save(job, checkpoint, "short_term_validate")
                raise ReviewRequired("invalid_short_term_output")
            try:
                return parse_output(checkpoint["raw"], source_ids=job["payload"]["source_ids"],
                    summary_chars=policy["summary_chars"], allowed_user_ids=checkpoint["user_ids"],
                    previous_summary=previous_summary, prompt_version=prompt_version)
            except (ValueError, TypeError) as exc:
                errors = validation_errors(exc, checkpoint["raw"], schema, RECOVERY_EXPLANATIONS,
                    context=json.loads(prompt[1]["content"]), pipeline="short_term")
                checkpoint["errors"] = errors
                await self._save(job, checkpoint, "short_term_validate")
                if (not getattr(self.config, "extraction_auto_regeneration", True)
                        or not correction_allowed(errors, RECOVERY_EXPLANATIONS)
                        or checkpoint.get("corrections", 0) >= checkpoint.get("correction_limit", 1)):
                    raise ReviewRequired("invalid_short_term_output") from None
                candidate_id = await self.jobs.archive_candidate(job["job_id"], job["lease_token"],
                    checkpoint, "invalid_short_term_output")
                checkpoint["corrections"] = checkpoint.get("corrections", 0) + 1
                checkpoint["regeneration"] = {"version": RECOVERY_VERSION, "candidate_id": candidate_id}
                checkpoint.pop("raw")
                checkpoint.pop("response_diagnostics", None)
                await self._save(job, checkpoint, "short_term_correcting")

    async def _generate(self, job, checkpoint, prompt, schema, deadline):
        if sum(len(message["content"].encode()) for message in prompt) > getattr(
            self.config, "extraction_max_prompt_bytes", 256000
        ):
            raise ReviewRequired("prompt_budget_exceeded")
        await self._save(job, checkpoint, "short_term_model")
        diagnostics = {}
        try:
            route = job["payload"].get("model_route")
            if route is None:
                route = {"provider_id": self._provider_id()}
            output_format = checkpoint.get("output_format", "text")
            raw = await self.adapter.generate(prompt, provider_id=route["provider_id"],
                output_format=output_format,
                output_schema=provider_schema(schema) if output_format == "json_schema" else None,
                **({"model": route["model_override"]} if route.get("model_override") else {}),
                timeout_seconds=max(.001, deadline - time.monotonic()), diagnostics=diagnostics)
        except Exception as exc:
            if is_transient_provider_error(exc):
                raise ModelNetworkError("provider_temporary_failure") from None
            raise ReviewRequired("provider_unavailable") from None
        if not isinstance(raw, str) or len(raw.encode()) > MAX_OUTPUT_BYTES:
            checkpoint["response_diagnostics"] = diagnostics
            checkpoint["errors"] = [{"path": "$", "type": "output_capacity_exceeded", "limit_bytes": MAX_OUTPUT_BYTES,
                **({"actual_bytes": len(raw.encode("utf-8"))} if isinstance(raw, str) else {})}]
            await self._save(job, checkpoint, "short_term_validate")
            raise ReviewRequired("output_budget_exceeded")
        checkpoint["raw"] = raw
        checkpoint["response_diagnostics"] = diagnostics
        await self._save(job, checkpoint, "short_term_validate")
