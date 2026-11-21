"""Measure the pinned host's retry boundary using HTTP simulation only.

These tests do not assess model quality or read provider credentials. Keeping
the real Context, Provider and OpenAI SDK exposes attempts that a fake adapter
would miss; MockTransport prevents any network traffic.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
from openai import APIConnectionError, InternalServerError
from tenacity import wait_none

from adapters.providers import ProviderAdapter
from scripts.verify_recorded_conversation import HttpBudget

MESSAGES = [
    {"role": "system", "content": "Synthetic extraction instructions."},
    {"role": "user", "content": "Synthetic earlier source."},
    {"role": "assistant", "content": "Synthetic earlier response."},
    {"role": "user", "content": "Synthetic current source."},
]


def completion():
    return httpx.Response(200, json={
        "id": "synthetic-completion", "object": "chat.completion", "created": 1,
        "model": "synthetic-model", "choices": [{"index": 0,
            "message": {"role": "assistant", "content": "synthetic-output"},
            "finish_reason": "stop"}],
    })


def error(status, message):
    return httpx.Response(status, json={
        "error": {"message": message, "type": "synthetic_error", "code": None},
    })


@asynccontextmanager
async def real_host(monkeypatch, handler, *, hook=None, single_attempt=False):
    from astrbot.core.provider.sources import request_retry
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
    from astrbot.core.star.context import Context

    # Remove only waiting; preserve the pinned host's retry decisions and count.
    monkeypatch.setattr(request_retry, "wait_exponential", lambda **kwargs: wait_none())
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler),
        event_hooks={"request": [hook] if hook else []}, trust_env=False)
    monkeypatch.setattr(ProviderOpenAIOfficial, "_create_http_client",
                        staticmethod(lambda config: client))
    provider = ProviderOpenAIOfficial({"id": "synthetic-chat", "type": "openai_chat_completion",
        "model": "synthetic-model", "api_base": "https://synthetic.invalid/v1",
        "key": ["synthetic-key"], "timeout": 5}, {})
    providers = {"synthetic-chat": provider}

    async def resolve(provider_id):
        return providers.get(provider_id)

    host = object.__new__(Context)
    host.provider_manager = SimpleNamespace(inst_map=providers, provider_insts=[provider],
        embedding_provider_insts=[], get_provider_by_id=resolve)
    if single_attempt:
        original = host.llm_generate

        async def bounded(**kwargs):
            return await original(**kwargs, request_max_retries=1)

        host.llm_generate = bounded
    try:
        yield ProviderAdapter(host, llm_concurrency=1), provider
    finally:
        await provider.terminate()


@pytest.mark.asyncio
@pytest.mark.parametrize("single_attempt,expected_requests", [(False, 5), (True, 1)])
async def test_sdk_zero_retries_does_not_remove_host_request_retries(
    monkeypatch, single_attempt, expected_requests,
):
    requests = []

    async def unavailable(request):
        requests.append(request)
        return error(503, "Synthetic upstream unavailable")

    async with real_host(monkeypatch, unavailable, single_attempt=single_attempt) as (adapter, provider):
        assert provider.client.max_retries == 0
        with pytest.raises(InternalServerError):
            await adapter.generate(MESSAGES, provider_id="synthetic-chat")
    assert len(requests) == expected_requests


@pytest.mark.asyncio
async def test_single_request_attempt_still_allows_host_context_fallback(monkeypatch):
    payloads = []

    async def context_failure(request):
        payloads.append(json.loads(request.content))
        return error(400, "maximum context length exceeded") if len(payloads) == 1 else completion()

    async with real_host(monkeypatch, context_failure, single_attempt=True) as (adapter, _):
        assert await adapter.generate(MESSAGES, provider_id="synthetic-chat") == "synthetic-output"
    assert len(payloads) == 2
    assert payloads[0]["messages"] == MESSAGES
    assert payloads[1]["messages"] == [MESSAGES[0], MESSAGES[-1]]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["context", "transient"])
async def test_http_budget_prevents_second_transport_dispatch(monkeypatch, failure):
    budget = HttpBudget(2)
    budget.active_call = True
    requests = []

    async def first_request(request):
        requests.append(request)
        if failure == "context":
            return error(400, "maximum context length exceeded")
        return error(503, "Synthetic upstream unavailable")

    async with real_host(monkeypatch, first_request, hook=budget.count_request) as (adapter, _):
        with pytest.raises(APIConnectionError):
            await adapter.generate(MESSAGES, provider_id="synthetic-chat")
    # An HTTPX hook exception becomes an SDK APIConnectionError. The host may
    # retry that exception, but no attempt beyond the admitted one is sent.
    assert len(requests) == budget.count == budget.call_count == 1


@pytest.mark.asyncio
async def test_cancellation_reaches_transport_and_releases_provider_slot(monkeypatch):
    entered, cancelled = asyncio.Event(), asyncio.Event()
    requests = []

    async def pending_then_complete(request):
        requests.append(request)
        if len(requests) > 1:
            return completion()
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with real_host(monkeypatch, pending_then_complete) as (adapter, _):
        request_task = asyncio.create_task(adapter.generate(MESSAGES, provider_id="synthetic-chat"))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            request_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request_task
            assert cancelled.is_set() and len(requests) == 1
            assert await adapter.generate(
                MESSAGES, provider_id="synthetic-chat", timeout_seconds=2,
            ) == "synthetic-output"
        finally:
            request_task.cancel()
            await asyncio.gather(request_task, return_exceptions=True)
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_adapter_deadline_cancels_real_host_without_another_attempt(monkeypatch):
    cancelled = asyncio.Event()
    requests = []

    async def pending(request):
        requests.append(request)
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with real_host(monkeypatch, pending) as (adapter, _):
        with pytest.raises(TimeoutError):
            await adapter.generate(MESSAGES, provider_id="synthetic-chat", timeout_seconds=1)
    assert cancelled.is_set() and len(requests) == 1
