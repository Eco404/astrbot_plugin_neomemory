"""Per-request host truncation controls use the provider actually dispatched."""

import asyncio
from types import SimpleNamespace

import pytest

from adapters.providers import (
    InputPolicyUnsupportedError,
    ProviderAdapter,
    ProviderUnavailableError,
)

MESSAGES = [{"role": "user", "content": "Independent synthetic source text."}]


class LegacyProvider:
    def __init__(self, provider_id="chat"):
        self.provider_config = {"id": provider_id}
        self.calls = []
        self.entered = asyncio.Event()
        self.release = None
        self.cancelled = False

    async def text_chat(self, **kwargs):
        self.calls.append(kwargs)
        self.entered.set()
        try:
            if self.release is not None:
                await self.release.wait()
            return SimpleNamespace(completion_text="provider output")
        except asyncio.CancelledError:
            self.cancelled = True
            raise


class CapableProvider(LegacyProvider):
    def __init__(self, provider_id="chat", *, capabilities=None):
        super().__init__(provider_id)
        self.capabilities = (
            frozenset({"no_context_truncation.v1"})
            if capabilities is None else capabilities
        )

    def get_request_capabilities(self):
        return self.capabilities


class Context:
    def __init__(self, *providers):
        self.providers = {provider.provider_config["id"]: provider for provider in providers}
        self.calls = []

    def get_provider_by_id(self, provider_id):
        return self.providers.get(provider_id)

    def get_all_providers(self):
        return list(self.providers.values())

    def get_all_embedding_providers(self):
        return []

    async def llm_generate(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(completion_text="context output")


@pytest.mark.parametrize("provider,expected", [
    (LegacyProvider(), False),
    (CapableProvider(capabilities=frozenset()), False),
    (CapableProvider(capabilities=frozenset({"another_capability.v1"})), False),
    (CapableProvider(), True),
])
def test_support_is_discovered_from_public_capability(provider, expected):
    assert ProviderAdapter(Context(provider)).supports_no_context_truncation("chat") is expected


@pytest.mark.asyncio
async def test_strict_unsupported_provider_rejected_without_request():
    provider = LegacyProvider()
    context = Context(provider)
    adapter = ProviderAdapter(context)
    with pytest.raises(InputPolicyUnsupportedError) as failure:
        await adapter.generate(MESSAGES, provider_id="chat", require_no_context_truncation=True)
    assert isinstance(failure.value, ProviderUnavailableError)
    assert context.calls == []
    assert provider.calls == []


@pytest.mark.asyncio
async def test_strict_dispatch_is_bound_to_checked_provider_instance():
    provider = CapableProvider()
    context = Context(provider)
    adapter = ProviderAdapter(context)
    assert await adapter.generate(
        MESSAGES, provider_id="chat", require_no_context_truncation=True,
    ) == "provider output"
    assert provider.calls == [{"contexts": MESSAGES, "allow_context_truncation": False}]
    assert context.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_compatible_request_retains_context_path_without_extra_keyword(explicit):
    provider = CapableProvider()
    context = Context(provider)
    kwargs = {"require_no_context_truncation": False} if explicit else {}
    result = await ProviderAdapter(context).generate(MESSAGES, provider_id="chat", **kwargs)
    assert result == "context output"
    assert context.calls == [{"chat_provider_id": "chat", "contexts": MESSAGES}]
    assert provider.calls == []


@pytest.mark.asyncio
async def test_queued_strict_request_rechecks_replacement_before_dispatch():
    blocker = CapableProvider("blocker")
    blocker.release = asyncio.Event()
    original = CapableProvider()
    replacement = LegacyProvider()
    context = Context(blocker, original)
    adapter = ProviderAdapter(context, llm_concurrency=1)
    first = asyncio.create_task(adapter.generate(
        MESSAGES, provider_id="blocker", require_no_context_truncation=True,
    ))
    queued = None
    try:
        await asyncio.wait_for(blocker.entered.wait(), 2)
        queued = asyncio.create_task(adapter.generate(
            MESSAGES, provider_id="chat", require_no_context_truncation=True,
        ))
        await asyncio.sleep(0)
        assert not queued.done()
        context.providers["chat"] = replacement
        blocker.release.set()
        assert await asyncio.wait_for(first, 2) == "provider output"
        with pytest.raises(InputPolicyUnsupportedError):
            await asyncio.wait_for(queued, 2)
        assert original.calls == []
        assert replacement.calls == []
        assert context.calls == []
    finally:
        tasks = [task for task in (first, queued) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_strict_call_releases_slot_and_does_not_retry():
    provider = CapableProvider()
    provider.release = asyncio.Event()
    context = Context(provider)
    adapter = ProviderAdapter(context, llm_concurrency=1)
    task = asyncio.create_task(adapter.generate(
        MESSAGES, provider_id="chat", require_no_context_truncation=True,
    ))
    try:
        await asyncio.wait_for(provider.entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert provider.cancelled
        assert len(provider.calls) == 1
        provider.release = None
        result = await asyncio.wait_for(adapter.generate(
            MESSAGES, provider_id="chat", require_no_context_truncation=True,
        ), 2)
        assert result == "provider output"
        assert len(provider.calls) == 2
        assert context.calls == []
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
