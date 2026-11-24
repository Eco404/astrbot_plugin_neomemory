"""Provider connectivity probes using synthetic inputs and shared adapter limits."""

from __future__ import annotations

import copy
import hashlib
import time
import uuid
from typing import Any

from .attempts import observed_call
from .jobs import ReviewRequired
from .provider_errors import http_status


def _text(value, *, maximum=256):
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > maximum:
        raise ValueError("invalid provider test field")
    return value


class ModelTestService:
    def __init__(self, plugin: Any):
        self.plugin = plugin
        # Workers keep their dependencies until JobRunner finishes/cancels them.
        # The plugin detaches its public attributes at shutdown to reject new
        # requests; that must not turn a completed probe into a provider error.
        self.adapter = plugin.runtime.adapter
        self.jobs = plugin.jobs
        self.config = plugin.config

    def _adapter(self):
        runtime = getattr(self.plugin, "runtime", None)
        if (not getattr(self.plugin, "ready", False) or getattr(self.plugin, "terminating", False)
                or runtime is None or getattr(self.plugin, "jobs", None) is None):
            raise RuntimeError("model_test_unavailable")
        return runtime.adapter

    def _fingerprint(self, kind, provider_id):
        adapter = self.adapter
        return (adapter.llm_fingerprint(provider_id) if kind == "llm"
                else adapter.embedding_fingerprint(provider_id))

    async def probe(self, *, kind, provider_id):
        if kind not in {"llm", "embedding"}:
            raise ValueError("invalid provider capability")
        _text(provider_id)
        service = copy.copy(self)
        service.adapter = self._adapter()
        fingerprint = service._fingerprint(kind, provider_id)
        return await service._measure(kind, provider_id, fingerprint)

    async def enqueue(self, *, kind, provider_id, idempotency_key, replaces=None):
        if kind not in {"llm", "embedding"}:
            raise ValueError("invalid provider capability")
        _text(provider_id)
        _text(idempotency_key, maximum=200)
        self._adapter()  # Admission only; running workers use stable dependencies.
        fingerprint = self._fingerprint(kind, provider_id)
        payload = {"kind": kind, "provider_id": provider_id, "fingerprint": fingerprint}
        scope = "provider_test:" + hashlib.sha256(provider_id.encode()).hexdigest()
        return await self.jobs.submit("model_test", scope, payload,
                                      "model_test:" + idempotency_key, max_attempts=1, replaces=replaces)

    async def retry(self, job_id, *, fresh=False):
        self._adapter()
        job = await self.jobs.get(job_id)
        if not job or job["kind"] != "model_test":
            raise ValueError("unknown model test")
        if not fresh:
            return await self.jobs.retry(job_id)
        # A deliberate new test may use a changed provider configuration.
        if job["state"] not in {"failed", "cancelled"}:
            raise ValueError("model test must be stopped before reprepare")
        return await self.enqueue(kind=job["payload"]["kind"], provider_id=job["payload"]["provider_id"],
                                  idempotency_key="fresh:" + uuid.uuid4().hex, replaces=job)

    async def _save(self, job, checkpoint, *, step, done):
        await self.jobs.save_checkpoint(job["job_id"], job["lease_token"], checkpoint=checkpoint,
                                               step=step, progress_done=done, progress_total=1)

    async def run(self, job):
        return await copy.copy(self)._run(job)

    async def _run(self, job):
        payload = job["payload"]
        kind, provider_id = payload["kind"], payload["provider_id"]
        checkpoint = job.get("checkpoint") or {}
        if "result" in checkpoint:
            return checkpoint["result"]
        await self._save(job, {}, step="model_test_call", done=0)
        result = await self._measure(kind, provider_id, payload["fingerprint"])
        await self._save(job, {"result": result}, step="model_test_complete", done=1)
        return result

    async def _measure(self, kind, provider_id, fingerprint):
        adapter = self.adapter
        try:
            if self._fingerprint(kind, provider_id) != fingerprint:
                raise ReviewRequired("model_test_configuration_changed")
            choices = adapter.list_models()[kind]
            model = next(item["model"] for item in choices if item["provider_id"] == provider_id)
        except ReviewRequired:
            raise
        except Exception:
            raise ReviewRequired("model_test_provider_unavailable") from None
        started = time.monotonic()
        timeout = self.config.extraction_timeout_seconds
        dimension = None
        diagnostics = {}
        try:
            if kind == "llm":
                await observed_call(adapter.generate, [{"role": "user", "content": "Reply OK."}],
                                    provider_id=provider_id, timeout_seconds=timeout, diagnostics=diagnostics)
            else:
                vectors = await observed_call(adapter.embed_batch, ["NeoMemory connectivity test"],
                                              provider_id=provider_id, timeout_seconds=timeout, diagnostics=diagnostics)
                dimension = len(vectors[0])
            if self._fingerprint(kind, provider_id) != fingerprint:
                raise ReviewRequired("model_test_configuration_changed")
        except ReviewRequired:
            raise
        except Exception as exc:
            code = "model_test_timeout" if isinstance(exc, TimeoutError) else "model_test_provider_failed"
            error = ReviewRequired(code)
            error.failure_stage = diagnostics.get("failure_stage") or (
                "provider_call" if diagnostics.get("call_started") else "provider_setup")
            error.http_status = http_status(exc)
            raise error from None
        result = {"ok": True, "kind": kind, "provider_id": provider_id, "model": model,
                  "elapsed_ms": round((time.monotonic() - started) * 1000, 2)}
        if dimension is not None:
            result["dimension"] = dimension
        return result
