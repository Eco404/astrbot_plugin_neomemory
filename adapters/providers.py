"""Use AstrBot's public provider interfaces without another retry loop."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from contextlib import AsyncExitStack
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


class ProviderUnavailableError(ValueError):
    """The configured host provider is missing or has the wrong capability."""


class InputPolicyUnsupportedError(ProviderUnavailableError):
    """The host cannot disable context truncation for this request."""


class VisionUnsupportedError(ProviderUnavailableError):
    """The configured model cannot accept images for this extraction request."""


def _unsupported_image_input(exc):
    if getattr(exc, "status_code", None) not in (400, 415, 422):
        return False
    body = getattr(exc, "body", None)
    if isinstance(body, Mapping) and isinstance(body.get("error"), Mapping):
        body = body["error"]
    message = str(body.get("message", "") if isinstance(body, Mapping) else body or "").lower()
    if getattr(exc, "code", None) in {"unsupported_image_input", "unsupported_modality"}:
        return True
    identifies_image = any(term in message for term in ("image", "vision", "图片", "图像"))
    rejects_modality = any(term in message for term in (
        "does not support image", "doesn't support image", "does not support vision",
        "image input is not supported", "image inputs are not supported", "image input not supported",
        "do not support image", "not a vision model", "only supported by certain models",
        "only supports text", "only support text", "text-only", "not supported by this model",
        "不支持图片", "不支持图像",
    ))
    return identifies_image and rejects_modality


class StructuredOutputUnsupportedError(ProviderUnavailableError):
    """The selected host provider has no supported structured-output route."""


def _structured_provider(provider: Any, output_format: str, output_schema: dict | None):
    """Own a separate host client so per-request output settings cannot leak."""
    from astrbot.core.provider.sources.openai_responses_source import (
        ProviderOpenAIResponses,
    )

    if type(provider) is not ProviderOpenAIResponses:
        raise StructuredOutputUnsupportedError("structured output requires the OpenAI Responses provider")
    config = copy.deepcopy(dict(_provider_config(provider)))
    config["model"] = provider.get_model()
    config["model"] = _provider_model(provider)
    extra = config.setdefault("custom_extra_body", {})
    text = extra.setdefault("text", {})
    if output_format == "json_schema":
        if not isinstance(output_schema, dict):
            raise ValueError("json_schema requires output_schema")
        text["format"] = {"type": "json_schema", "name": "neomemory_extraction",
                          "strict": True, "schema": copy.deepcopy(output_schema)}
    else:
        text["format"] = {"type": "json_object"}
    return ProviderOpenAIResponses(config, copy.deepcopy(provider.provider_settings))


def _unsupported_schema_format(exc: Exception) -> bool:
    if isinstance(exc, StructuredOutputUnsupportedError):
        return True
    if getattr(exc, "status_code", None) not in (400, 422):
        return False
    param = getattr(exc, "param", None)
    code = getattr(exc, "code", None)
    if param in {"text.format", "text.format.type", "response_format", "response_format.type"}:
        if code in {"unsupported_parameter", "unsupported_value", "unknown_parameter"}:
            return True
    body = getattr(exc, "body", None)
    message = str(body.get("message", "") if isinstance(body, Mapping) else body or "").lower()
    if code == "invalid_json_schema" or "invalid schema" in message or (param and ".schema" in param):
        return False
    format_named = any(name in message for name in ("json_schema", "json schema", "structured output", "text.format", "response_format"))
    rejected = any(word in message for word in ("unsupported", "not supported", "does not support", "unknown parameter", "unrecognized", "不支持"))
    return format_named and rejected


def _response_diagnostics(response: Any) -> dict[str, Any]:
    raw = getattr(response, "raw_completion", None)
    def field(value, key):
        return value.get(key) if isinstance(value, Mapping) else getattr(value, key, None)
    result = {"response_id": field(raw, "id"), "response_status": field(raw, "status"),
              "incomplete_reason": field(field(raw, "incomplete_details"), "reason")}
    choices = field(raw, "choices")
    if choices:
        result["finish_reason"] = field(choices[0], "finish_reason")
    usage = field(raw, "usage")
    for name in ("input_tokens", "output_tokens", "prompt_tokens", "completion_tokens"):
        value = field(usage, name)
        if isinstance(value, int):
            result[name] = value
    return {key: value for key, value in result.items() if value is not None}


class EmbeddingValidationError(ValueError):
    """An embedding response cannot be safely used by cosine retrieval."""


def _positive_timeout(value: float) -> float:
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise ValueError("timeout_seconds must be finite and positive")
    return float(value)


def _provider_config(provider: Any) -> Mapping[str, Any]:
    config = getattr(provider, "provider_config", {})
    return config if isinstance(config, Mapping) else {}


def _provider_id(provider: Any) -> str:
    return str(_provider_config(provider).get("id") or "")


def _provider_model(provider: Any) -> str:
    # OpenAIEmbeddingProvider inherits get_model(), but keeps the actual model
    # in .model instead of AbstractProvider.model_name.
    config = _provider_config(provider)
    return str(
        provider.get_model()
        or getattr(provider, "model", "")
        or config.get("embedding_model")
        or config.get("model")
        or ""
    )


def _provider_class(provider: Any) -> str:
    return f"{type(provider).__module__}.{type(provider).__qualname__}"


def _dimension(provider: Any) -> int:
    value = provider.get_dim()
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EmbeddingValidationError("provider dimension must be a nonnegative integer")
    return value


def _endpoint_identity(value: Any) -> str:
    """Remove URL credentials and credential query parameters before hashing."""
    if not isinstance(value, str):
        return ""
    parsed = urlsplit(value)
    hostname = parsed.hostname or ""
    if ":" in hostname:
        hostname = f"[{hostname}]"
    port = f":{parsed.port}" if parsed.port else ""
    query = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not any(part in key.casefold() for part in ("key", "token", "secret", "auth", "signature", "password"))
    ]
    return urlunsplit((parsed.scheme, hostname + port, parsed.path, urlencode(sorted(query)), ""))


def normalize_vectors(
    vectors: Any, *, count: int, dimension: int = 0
) -> list[list[float]]:
    """Validate the complete response before exposing any vector to callers."""
    if not isinstance(vectors, (list, tuple)) or len(vectors) != count:
        raise EmbeddingValidationError("embedding response count does not match input")
    normalized: list[list[float]] = []
    expected = dimension
    for vector in vectors:
        if not isinstance(vector, (list, tuple)) or not vector:
            raise EmbeddingValidationError("embedding vectors must be nonempty sequences")
        if not expected:
            expected = len(vector)
        if len(vector) != expected:
            raise EmbeddingValidationError("embedding dimension mismatch")
        if any(isinstance(item, bool) or not isinstance(item, (int, float)) for item in vector):
            raise EmbeddingValidationError("embedding coordinates must be numeric")
        values = [float(item) for item in vector]
        if not all(math.isfinite(item) for item in values):
            raise EmbeddingValidationError("embedding coordinates must be finite")
        # Scale before taking the norm to avoid overflow on otherwise finite
        # coordinates. A zero vector cannot define cosine similarity.
        scale = max(abs(item) for item in values)
        if scale == 0:
            raise EmbeddingValidationError("embedding vector must be nonzero")
        scaled = [item / scale for item in values]
        norm = math.sqrt(math.fsum(item * item for item in scaled))
        normalized.append([item / norm for item in scaled])
    return normalized


class _ConcurrencyLimiter:
    """Track in-flight calls independently of a live concurrency limit."""

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._active = 0
        self._condition = asyncio.Condition()

    async def resize(self, limit: int) -> None:
        async with self._condition:
            self._limit = limit
            self._condition.notify_all()

    async def __aenter__(self) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self._active < self._limit)
            self._active += 1

    async def __aexit__(self, *exc_info: object) -> None:
        async with self._condition:
            self._active -= 1
            self._condition.notify_all()


def _validate_concurrency(*values: int) -> None:
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("provider concurrency must be a positive integer")


class ProviderAdapter:
    """Share independent concurrency budgets for chat and embedding calls.

    The timeout includes concurrency queueing. Cancellation propagates to the host
    call. Format negotiation and SDK/provider retries share the outer deadline.
    """

    def __init__(self, context: Any, *, llm_concurrency: int = 2, embedding_concurrency: int = 2) -> None:
        _validate_concurrency(llm_concurrency, embedding_concurrency)
        self.context = context
        self._llm_limiter = _ConcurrencyLimiter(llm_concurrency)
        self._embedding_limiter = _ConcurrencyLimiter(embedding_concurrency)

    async def reconfigure(self, *, llm_concurrency: int, embedding_concurrency: int) -> None:
        """Apply limits to queued and future calls without cancelling active calls."""
        _validate_concurrency(llm_concurrency, embedding_concurrency)
        await self._llm_limiter.resize(llm_concurrency)
        await self._embedding_limiter.resize(embedding_concurrency)

    def _resolve(self, provider_id: str, *, embedding: bool) -> Any:
        if not isinstance(provider_id, str) or not provider_id:
            raise ProviderUnavailableError("provider_id must be configured")
        provider = self.context.get_provider_by_id(provider_id)
        available = (
            self.context.get_all_embedding_providers()
            if embedding else self.context.get_all_providers()
        )
        if provider is None or not any(item is provider for item in available):
            raise ProviderUnavailableError("configured provider is unavailable for this capability")
        return provider

    def list_models(self) -> dict[str, list[dict[str, Any]]]:
        result: dict[str, list[dict[str, Any]]] = {"llm": [], "embedding": [], "rerank": []}
        for kind, providers in (
            ("llm", self.context.get_all_providers()),
            ("embedding", self.context.get_all_embedding_providers()),
            ("rerank", getattr(getattr(self.context, "provider_manager", None), "rerank_provider_insts", ())),
        ):
            for provider in providers:
                item: dict[str, Any] = {
                    "provider_id": _provider_id(provider),
                    "model": _provider_model(provider),
                    "provider_class": _provider_class(provider),
                }
                if kind == "embedding":
                    item["dimension"] = _dimension(provider)
                result[kind].append(item)
        return result

    async def rerank(self, query, documents, *, provider_id):
        provider = self.context.get_provider_by_id(provider_id)
        if provider is None or not callable(getattr(provider, "rerank", None)):
            raise ProviderUnavailableError("configured rerank provider unavailable")
        rows = await provider.rerank(query, documents, top_n=len(documents))
        # A raw logit of zero corresponds to a sigmoid score of 0.50.
        threshold = 0. if getattr(provider, "raw_scores", False) else .5
        return [row.index for row in sorted(rows, key=lambda row: -row.relevance_score)
                if row.relevance_score >= threshold]

    def embedding_dimension(self, provider_id: str) -> int:
        return _dimension(self._resolve(provider_id, embedding=True))

    def supports_no_context_truncation(self, provider_id: str) -> bool:
        provider = self._resolve(provider_id, embedding=False)
        capabilities = getattr(provider, "get_request_capabilities", None)
        return callable(capabilities) and "no_context_truncation.v1" in capabilities()

    def embedding_fingerprint(self, provider_id: str, revision: str = "") -> str:
        provider = self._resolve(provider_id, embedding=True)
        config = _provider_config(provider)
        endpoints = {
            key: _endpoint_identity(config[key])
            for key in ("embedding_api_base", "api_base", "base_url", "endpoint", "host", "ollama_host")
            if key in config
        }
        endpoint_hash = hashlib.sha256(
            json.dumps(endpoints, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        payload = {
            "provider_id": provider_id,
            "class": _provider_class(provider),
            "model": _provider_model(provider),
            "dimension": _dimension(provider),
            "endpoint_hash": endpoint_hash,
            "dimensions_mode": str(config.get("embedding_dimensions_mode", "")),
            "input_type": str(config.get("input_type", "")),
            "task_type": str(config.get("task_type", "")),
            "revision": str(revision),
        }
        return "embedding.v1:" + hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def llm_fingerprint(self, provider_id: str, revision: str = "") -> str:
        """Track semantic model changes without persisting credentials/config bodies."""
        provider = self._resolve(provider_id, embedding=False)
        config = _provider_config(provider)
        payload = {
            "provider_id": provider_id, "class": _provider_class(provider),
            "model": _provider_model(provider), "revision": str(revision),
            "endpoints": {key: _endpoint_identity(config[key]) for key in
                ("api_base", "base_url", "endpoint", "host", "ollama_host") if key in config},
            "generation": {key: config[key] for key in
                ("temperature", "top_p", "max_tokens", "reasoning_effort", "custom_extra_body")
                if key in config},
        }
        return "llm.v1:" + hashlib.sha256(json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()

    async def generate(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        provider_id: str,
        timeout_seconds: float = 60,
        model: str = "",
        require_no_context_truncation: bool = False,
        output_format: str = "text",
        output_schema: dict | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> str:
        timeout = _positive_timeout(timeout_seconds)
        self._resolve(provider_id, embedding=False)
        if output_format not in {"text", "json_object", "json_schema"}:
            raise ValueError("unsupported output_format")
        contexts = []
        has_images = False
        for message in messages:
            role, content = message.get("role"), message.get("content")
            if isinstance(content, list) and role == "user" and content:
                if not all(isinstance(part, Mapping) and part.get("type") in {"text", "image_url"} for part in content):
                    raise ValueError("unknown content part in user message")
                has_images |= any(part["type"] == "image_url" for part in content)
            elif role not in {"system", "user", "assistant"} or not isinstance(content, str):
                raise ValueError("messages require supported roles and content")
            contexts.append({"role": role, "content": content})
        if not contexts:
            raise ValueError("messages must not be empty")
        loop = asyncio.get_running_loop()
        queued_at = loop.time()
        async with asyncio.timeout(timeout), AsyncExitStack() as stack:
            try:
                await stack.enter_async_context(self._llm_limiter)
            finally:
                if diagnostics is not None:
                    diagnostics.update(queue_seconds=loop.time() - queued_at, call_started=False)
            started = loop.time()
            if diagnostics is not None:
                diagnostics["call_started"] = True
            owned = None
            provider = self._resolve(provider_id, embedding=False)
            modalities = _provider_config(provider).get("modalities")
            if has_images and modalities and "image" not in modalities:
                raise VisionUnsupportedError("selected provider declares no image input")
            if require_no_context_truncation:
                capabilities = getattr(provider, "get_request_capabilities", None)
                if not callable(capabilities) or "no_context_truncation.v1" not in capabilities():
                    raise InputPolicyUnsupportedError("no_context_truncation.v1 is unavailable")
            model_options = {"model": model} if model else {}
            effective_format = output_format
            try:
                if output_format != "text":
                    try:
                        owned = _structured_provider(provider, output_format, output_schema)
                        kwargs = {"allow_context_truncation": False} if require_no_context_truncation else {}
                        response = await owned.text_chat(contexts=copy.deepcopy(contexts), **model_options, **kwargs)
                    except Exception as exc:
                        if output_format != "json_schema" or not _unsupported_schema_format(exc):
                            raise
                        effective_format = "text"
                if effective_format == "text":
                    if require_no_context_truncation:
                        response = await provider.text_chat(contexts=contexts, allow_context_truncation=False, **model_options)
                    else:
                        response = await self.context.llm_generate(chat_provider_id=provider_id, contexts=contexts, **model_options)
                if diagnostics is not None:
                    diagnostics.update(_response_diagnostics(response))
            except Exception as exc:
                if has_images and _unsupported_image_input(exc):
                    raise VisionUnsupportedError("model rejected image input") from None
                raise
            finally:
                if diagnostics is not None:
                    diagnostics.update(output_format=effective_format, requested_output_format=output_format,
                                       call_seconds=asyncio.get_running_loop().time() - started)
                if owned is not None:
                    await owned.terminate()
            content = getattr(response, "completion_text", None)
            if not isinstance(content, str) or not content.strip():
                raise ValueError("LLM response contains no completion text")
            return content

    async def embed_batch(
        self,
        texts: Sequence[str],
        *,
        provider_id: str,
        timeout_seconds: float = 30,
        diagnostics: dict[str, Any] | None = None,
    ) -> list[list[float]]:
        timeout = _positive_timeout(timeout_seconds)
        provider = self._resolve(provider_id, embedding=True)
        if any(not isinstance(text, str) or not text.strip() for text in texts):
            raise ValueError("embedding inputs must contain nonempty text")
        if not texts:
            return []
        loop = asyncio.get_running_loop()
        queued_at = loop.time()
        stage = "provider_queue"
        acquired = False
        if diagnostics is not None:
            diagnostics.update(queue_seconds=0., call_seconds=0., call_started=False, completed=False)
        try:
            async with asyncio.timeout(timeout):
                async with self._embedding_limiter:
                    acquired = True
                    if diagnostics is not None:
                        diagnostics["queue_seconds"] = loop.time() - queued_at
                        diagnostics["call_started"] = True
                    stage = "provider_call"
                    call_started = loop.time()
                    try:
                        vectors = await provider.get_embeddings(list(texts))
                    finally:
                        if diagnostics is not None:
                            diagnostics["call_seconds"] = loop.time() - call_started
                    stage = "response_validation"
                    result = normalize_vectors(vectors, count=len(texts), dimension=_dimension(provider))
                    if diagnostics is not None:
                        diagnostics["completed"] = True
                    return result
        except BaseException as exc:
            if diagnostics is not None:
                diagnostics.update(failure_stage=stage, error_type=type(exc).__name__)
            raise
        finally:
            if diagnostics is not None and not acquired:
                diagnostics["queue_seconds"] = loop.time() - queued_at
