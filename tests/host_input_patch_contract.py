"""Explicit-only contract tests for the proposed isolated AstrBot host patch.

This filename is intentionally outside pytest's default test_*.py collection.
Run it with the patched worktree first on PYTHONPATH; the current production
host does not implement this proposed API. All transport is synthetic.
"""

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
from openai import BadRequestError
from tenacity import wait_none

from adapters.providers import ProviderAdapter
from tests.test_fidelity_retry_boundary import completion, error
from tests.test_host_input_integrity import extraction_window as _extraction_window
from tests.test_host_input_integrity import no_change_completion, source_ids

extraction_window = _extraction_window

MESSAGES = [{"role": "system", "content": "Synthetic rules"}, {"role": "user", "content": "Synthetic source"}]


def success(kind, text="synthetic-output"):
    if kind == "chat":
        data = json.loads(completion().content)
        data["choices"][0]["message"]["content"] = text
    else:
        data = {"id": "synthetic-response", "object": "response", "created_at": 1,
            "status": "completed", "model": "synthetic-model", "output": [{
                "id": "synthetic-message", "type": "message", "status": "completed",
                "role": "assistant", "content": [{"type": "output_text", "text": text, "annotations": []}],
            }], "parallel_tool_calls": True, "tool_choice": "auto", "tools": []}
    return httpx.Response(200, json=data)


def payload_messages(payload, kind):
    messages = payload["messages" if kind == "chat" else "input"]
    return [{"role": message["role"], "content": message["content"]} for message in messages]


@asynccontextmanager
async def patched_host(monkeypatch, handler, kind):
    from astrbot.core.provider.sources import request_retry
    from astrbot.core.provider.sources.openai_responses_source import (
        ProviderOpenAIResponses,
    )
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
    from astrbot.core.star.context import Context

    assert hasattr(ProviderOpenAIOfficial, "get_request_capabilities"), "use the isolated patched host"
    monkeypatch.setattr(request_retry, "wait_exponential", lambda **kwargs: wait_none())
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
    monkeypatch.setattr(ProviderOpenAIOfficial, "_create_http_client", staticmethod(lambda config: client))
    cls = ProviderOpenAIOfficial if kind == "chat" else ProviderOpenAIResponses
    provider = cls({"id": "synthetic-chat", "type": "openai_chat_completion" if kind == "chat" else "openai_responses",
        "model": "synthetic-model", "api_base": "https://synthetic.invalid/v1", "key": ["synthetic-key"], "timeout": 5}, {})
    providers = {"synthetic-chat": provider}

    async def resolve(provider_id):
        return providers.get(provider_id)

    context = object.__new__(Context)
    context.provider_manager = SimpleNamespace(inst_map=providers, provider_insts=[provider],
        embedding_provider_insts=[], get_provider_by_id=resolve)
    try:
        yield context, provider
    finally:
        await provider.terminate()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chat", "responses"])
@pytest.mark.parametrize("strict", [False, True])
async def test_per_call_flag_preserves_default_and_strict_blocks_before_fallback(monkeypatch, kind, strict):
    payloads = []

    async def handler(request):
        payloads.append(json.loads(request.content))
        return error(400, "maximum context length exceeded") if len(payloads) == 1 else success(kind)

    async with patched_host(monkeypatch, handler, kind) as (context, provider):
        before = copy.deepcopy(provider.provider_config)
        assert "no_context_truncation.v1" in provider.get_request_capabilities()
        kwargs = {"allow_context_truncation": False} if strict else {}
        if strict:
            with pytest.raises(BadRequestError, match="maximum context length"):
                await context.llm_generate(chat_provider_id="synthetic-chat", contexts=MESSAGES, **kwargs)
        else:
            assert (await context.llm_generate(chat_provider_id="synthetic-chat", contexts=MESSAGES)).completion_text == "synthetic-output"
        assert provider.provider_config == before
    assert len(payloads) == (1 if strict else 2)
    assert payload_messages(payloads[0], kind) == MESSAGES
    if not strict:
        assert payload_messages(payloads[1], kind) == MESSAGES[:1]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chat", "responses"])
async def test_strict_context_failure_never_consumes_extraction(monkeypatch, extraction_window, kind):
    memory, pipeline, service, batch = extraction_window
    calls = []

    async def handler(request):
        calls.append(request)
        return error(400, "maximum context length exceeded")

    async with patched_host(monkeypatch, handler, kind) as (context, _):
        adapter = ProviderAdapter(context)

        async def model(messages):
            return await adapter.generate(messages, provider_id="synthetic-chat",
                require_no_context_truncation=True)

        with pytest.raises(BadRequestError):
            await service.run(batch, model, model_id="synthetic-model")
    assert len(calls) == 1
    assert await memory.get_extraction_batch(batch.batch_id) is None
    assert await pipeline.consumed(batch.generation_id, source_ids(batch)) == set()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chat", "responses"])
async def test_adapter_strict_complete_input_commits_without_control_leaking_to_api(
    monkeypatch, extraction_window, kind,
):
    memory, pipeline, service, batch = extraction_window
    payloads = []

    async def handler(request):
        payloads.append(json.loads(request.content))
        content = json.loads(no_change_completion().content)["choices"][0]["message"]["content"]
        return success(kind, text=content)

    async with patched_host(monkeypatch, handler, kind) as (context, _):
        adapter = ProviderAdapter(context)
        assert adapter.supports_no_context_truncation("synthetic-chat")

        async def model(messages):
            return await adapter.generate(messages, provider_id="synthetic-chat",
                require_no_context_truncation=True)

        await service.run(batch, model, model_id="synthetic-model")
    assert len(payloads) == 1
    assert "allow_context_truncation" not in payloads[0]
    assert payload_messages(payloads[0], kind) == batch.messages()
    assert await memory.get_extraction_batch(batch.batch_id) is not None
    assert await pipeline.consumed(batch.generation_id, source_ids(batch)) == set(source_ids(batch))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chat", "responses"])
async def test_strict_keeps_network_retry_payload_unchanged(monkeypatch, kind):
    payloads = []

    async def handler(request):
        payloads.append(json.loads(request.content))
        return error(503, "Synthetic unavailable") if len(payloads) == 1 else success(kind)

    async with patched_host(monkeypatch, handler, kind) as (context, _):
        result = await context.llm_generate(chat_provider_id="synthetic-chat", contexts=MESSAGES,
            allow_context_truncation=False)
        assert result.completion_text == "synthetic-output"
    assert len(payloads) == 2
    assert all(payload_messages(payload, kind) == MESSAGES for payload in payloads)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chat", "responses"])
async def test_strict_stream_blocks_initial_context_error(monkeypatch, kind):
    calls = []

    async def handler(request):
        calls.append(request)
        return error(400, "maximum context length exceeded")

    async with patched_host(monkeypatch, handler, kind) as (_, provider):
        with pytest.raises(BadRequestError):
            async for _ in provider.text_chat_stream(contexts=MESSAGES, allow_context_truncation=False):
                pytest.fail("no response should precede the context error")
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chat", "responses"])
async def test_invalid_flag_rejected_before_dispatch(monkeypatch, kind):
    calls = []

    async def handler(request):
        calls.append(request)
        return success(kind)

    async with patched_host(monkeypatch, handler, kind) as (context, _):
        with pytest.raises(TypeError, match="must be a bool"):
            await context.llm_generate(chat_provider_id="synthetic-chat", contexts=MESSAGES,
                allow_context_truncation="false")
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chat", "responses"])
async def test_strict_empty_output_is_error(monkeypatch, kind):
    from astrbot.core.exceptions import EmptyModelOutputError

    calls = []

    async def handler(request):
        calls.append(request)
        return success(kind, text="")

    async with patched_host(monkeypatch, handler, kind) as (context, _):
        with pytest.raises(EmptyModelOutputError):
            await context.llm_generate(chat_provider_id="synthetic-chat", contexts=MESSAGES,
                allow_context_truncation=False)
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chat", "responses"])
async def test_cancelled_strict_request_does_not_retry(monkeypatch, kind):
    entered, cancelled = asyncio.Event(), asyncio.Event()
    calls = []

    async def handler(request):
        calls.append(request)
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with patched_host(monkeypatch, handler, kind) as (context, _):
        task = asyncio.create_task(context.llm_generate(chat_provider_id="synthetic-chat", contexts=MESSAGES,
            allow_context_truncation=False))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert cancelled.is_set() and len(calls) == 1
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chat", "responses"])
async def test_concurrent_flags_do_not_leak_into_shared_provider(monkeypatch, kind):
    entered, release = asyncio.Event(), asyncio.Event()
    payloads = []
    strict_messages = [{"role": "system", "content": "strict"}, MESSAGES[1]]
    default_messages = [{"role": "system", "content": "default"}, MESSAGES[1]]

    async def handler(request):
        payload = json.loads(request.content)
        payloads.append(payload)
        messages = payload_messages(payload, kind)
        if messages == strict_messages:
            entered.set()
            await release.wait()
            return error(400, "maximum context length exceeded")
        if messages == default_messages:
            return error(400, "maximum context length exceeded")
        return success(kind)

    async with patched_host(monkeypatch, handler, kind) as (context, provider):
        before = copy.deepcopy(provider.provider_config)
        task = asyncio.create_task(context.llm_generate(chat_provider_id="synthetic-chat", contexts=strict_messages,
            allow_context_truncation=False))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            default = await asyncio.wait_for(context.llm_generate(chat_provider_id="synthetic-chat", contexts=default_messages), 2)
            assert default.completion_text == "synthetic-output"
            release.set()
            with pytest.raises(BadRequestError):
                await asyncio.wait_for(task, 2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert provider.provider_config == before
    assert [payload_messages(payload, kind) for payload in payloads] == [strict_messages, default_messages, default_messages[:1]]


def test_base_provider_does_not_claim_optional_control():
    from astrbot.core.provider.provider import Provider

    assert Provider.get_request_capabilities(SimpleNamespace()) == frozenset()


def test_overridden_entry_point_does_not_inherit_unverified_capability():
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial

    class DifferentProvider(ProviderOpenAIOfficial):
        async def text_chat(self, **kwargs):
            raise NotImplementedError

    assert object.__new__(DifferentProvider).get_request_capabilities() == frozenset()


@pytest.mark.parametrize("kind", ["chat", "responses"])
def test_overridden_query_with_inherited_entry_does_not_claim_verified_capability(kind):
    from astrbot.core.provider.sources.openai_responses_source import (
        ProviderOpenAIResponses,
    )
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial

    base = ProviderOpenAIOfficial if kind == "chat" else ProviderOpenAIResponses

    class DifferentProvider(base):
        async def _query(self, *args, **kwargs):
            raise NotImplementedError

    assert DifferentProvider.text_chat is base.text_chat
    assert object.__new__(DifferentProvider).get_request_capabilities() == frozenset()
