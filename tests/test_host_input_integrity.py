"""Characterize the pinned host's unresolved input-integrity boundary offline.

These are defect reproductions, not assertions that production is protected.
The real extraction transaction, Context, Provider and SDK are exercised with
synthetic input and an HTTP MockTransport. No provider credentials are read.
"""

import asyncio
import copy
import hashlib
import json

import httpx
import pytest
import pytest_asyncio
from openai import BadRequestError

from adapters.providers import ProviderAdapter
from core.extraction_apply import ExtractionService
from core.memory_spaces import private_space
from storage.memory_store import MemoryStore
from storage.pipeline_store import PipelineStore
from storage.source_store import SourceStore
from tests.test_extraction_apply import capture
from tests.test_fidelity_retry_boundary import completion, error, real_host


def source_ids(batch):
    return tuple(source["source_id"] for source in batch.sources)


def no_change_completion():
    response = json.loads(completion().content)
    response["choices"][0]["message"]["content"] = json.dumps({
        "schema_version": "extraction.v1", "result": "no_change",
        "reason": "no_meaningful_content", "node_changes": [], "edges": [],
        "profile_changes": [], "relationship_change": None, "coverage_changes": [],
    })
    return httpx.Response(200, json=response)


@pytest_asyncio.fixture
async def extraction_window(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await sources.open()
    await memory.open()
    try:
        source = await capture(sources, "Synthetic input: a user reports a durable hobby.")
        space = await memory.get_or_create_space(private_space("p", source.logical_user_id))
        service = ExtractionService(sources, memory)
        batch = await service.prepare(space_id=space["space_id"],
            source_ids=(source.source_id,), persona="Synthetic evidence-sensitive companion.")
        yield memory, PipelineStore(memory), service, batch
    finally:
        await memory.close()
        await sources.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("single_attempt", [False, True])
async def test_context_fallback_can_consume_window_after_removing_all_source_data(
    monkeypatch, extraction_window, single_attempt,
):
    """Known defect: a syntactically valid no_change cannot prove input delivery."""
    memory, pipeline, service, batch = extraction_window
    payloads = []
    caller_messages = []
    before = batch.messages()
    assert [message["role"] for message in before] == ["system", "user"]

    async def context_failure(request):
        payloads.append(json.loads(request.content))
        return error(400, "maximum context length exceeded") if len(payloads) == 1 else no_change_completion()

    async with real_host(monkeypatch, context_failure, single_attempt=single_attempt) as (adapter, provider):
        config_before = copy.deepcopy(provider.provider_config)

        async def model(messages):
            caller_messages.extend(messages)
            return await adapter.generate(messages, provider_id="synthetic-chat")

        receipt = await service.run(batch, model, model_id="synthetic-model")
        assert provider.provider_config == config_before

    assert len(payloads) == 2
    assert payloads[0]["messages"] == before
    assert payloads[1]["messages"] == [before[0]]
    # Both the caller's object and the trusted snapshot look unchanged: a
    # post-call local comparison would miss the host's private deep copy.
    assert caller_messages == before == batch.messages()
    assert receipt["input_digest"] == batch.input_digest
    assert receipt["activity_round"] == 1
    assert await memory.get_extraction_batch(batch.batch_id) is not None
    assert await pipeline.consumed(batch.generation_id, source_ids(batch)) == set(source_ids(batch))
    assert await memory.list_nodes(space_id=batch.space["space_id"]) == []


@pytest.mark.asyncio
async def test_transient_network_retry_keeps_full_extraction_input(monkeypatch, extraction_window):
    memory, pipeline, service, batch = extraction_window
    payloads = []

    async def transient(request):
        payloads.append(json.loads(request.content))
        return error(503, "Synthetic service unavailable") if len(payloads) == 1 else no_change_completion()

    async with real_host(monkeypatch, transient) as (adapter, _):
        async def model(messages):
            return await adapter.generate(messages, provider_id="synthetic-chat")

        await service.run(batch, model, model_id="synthetic-model")
    assert len(payloads) == 2
    assert all(payload["messages"] == batch.messages() for payload in payloads)
    assert await memory.get_extraction_batch(batch.batch_id) is not None
    assert await pipeline.consumed(batch.generation_id, source_ids(batch)) == set(source_ids(batch))


@pytest.mark.asyncio
async def test_persistent_context_error_repeats_system_only_until_host_limit(
    monkeypatch, extraction_window,
):
    memory, pipeline, service, batch = extraction_window
    payloads = []

    async def too_long(request):
        payloads.append(json.loads(request.content))
        return error(400, "maximum context length exceeded")

    async with real_host(monkeypatch, too_long, single_attempt=True) as (adapter, _):
        async def model(messages):
            return await adapter.generate(messages, provider_id="synthetic-chat")

        with pytest.raises(BadRequestError, match="maximum context length"):
            await service.run(batch, model, model_id="synthetic-model")
    assert len(payloads) == 10
    assert payloads[0]["messages"] == batch.messages()
    assert all(payload["messages"] == [batch.messages()[0]] for payload in payloads[1:])
    assert await memory.get_extraction_batch(batch.batch_id) is None
    assert await pipeline.consumed(batch.generation_id, source_ids(batch)) == set()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["empty", "invalid_json", "bad_request"])
async def test_failed_fallback_output_does_not_consume_sources(monkeypatch, extraction_window, failure):
    from astrbot.core.exceptions import EmptyModelOutputError

    memory, pipeline, service, batch = extraction_window
    payloads = []

    async def failed(request):
        payloads.append(json.loads(request.content))
        if len(payloads) == 1:
            return error(400, "maximum context length exceeded")
        if failure == "bad_request":
            return error(400, "Synthetic malformed request")
        response = json.loads(completion().content)
        response["choices"][0]["message"]["content"] = "" if failure == "empty" else "not json"
        return httpx.Response(200, json=response)

    expected = {"empty": EmptyModelOutputError, "invalid_json": ValueError, "bad_request": BadRequestError}[failure]
    async with real_host(monkeypatch, failed) as (adapter, _):
        async def model(messages):
            return await adapter.generate(messages, provider_id="synthetic-chat")

        with pytest.raises(expected):
            await service.run(batch, model, model_id="synthetic-model")
    assert len(payloads) == 2
    assert payloads[1]["messages"] == [batch.messages()[0]]
    assert await memory.get_extraction_batch(batch.batch_id) is None
    assert await pipeline.consumed(batch.generation_id, source_ids(batch)) == set()
    assert (await memory.get_active_generation(batch.space["space_id"]))["activity_round"] == 0


@pytest.mark.asyncio
async def test_cancelled_content_fallback_keeps_window_pending_and_releases_slot(
    monkeypatch, extraction_window,
):
    memory, pipeline, service, batch = extraction_window
    entered, cancelled = asyncio.Event(), asyncio.Event()
    payloads = []

    async def pending(request):
        payloads.append(json.loads(request.content))
        if len(payloads) == 1:
            return error(400, "maximum context length exceeded")
        if len(payloads) > 2:
            return no_change_completion()
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with real_host(monkeypatch, pending) as (adapter, _):
        async def model(messages):
            return await adapter.generate(messages, provider_id="synthetic-chat")

        task = asyncio.create_task(service.run(batch, model, model_id="synthetic-model"))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert cancelled.is_set()
            assert await memory.get_extraction_batch(batch.batch_id) is None
            assert await pipeline.consumed(batch.generation_id, source_ids(batch)) == set()
            assert len(payloads) == 2
            # A later, explicit attempt starts from the complete input.
            await service.run(batch, model, model_id="synthetic-model")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert len(payloads) == 3
    assert payloads[2]["messages"] == batch.messages()
    assert await pipeline.consumed(batch.generation_id, source_ids(batch)) == set(source_ids(batch))


@pytest.mark.asyncio
async def test_shared_provider_fallback_does_not_mutate_other_call(monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    payloads = []
    first = [{"role": "system", "content": "Synthetic rules A"}, {"role": "user", "content": "A source"}]
    second = [{"role": "system", "content": "Synthetic rules B"}, {"role": "user", "content": "B source"}]
    originals = copy.deepcopy((first, second))

    async def concurrent(request):
        payload = json.loads(request.content)
        payloads.append(payload)
        messages = payload["messages"]
        if messages == first:
            entered.set()
            await release.wait()
            return error(400, "maximum context length exceeded")
        return completion()

    async with real_host(monkeypatch, concurrent) as (original, provider):
        adapter = ProviderAdapter(original.context, llm_concurrency=2)
        config_before = copy.deepcopy(provider.provider_config)
        task = asyncio.create_task(adapter.generate(first, provider_id="synthetic-chat"))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            assert await adapter.generate(second, provider_id="synthetic-chat", timeout_seconds=2) == "synthetic-output"
            release.set()
            assert await asyncio.wait_for(task, 2) == "synthetic-output"
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert provider.provider_config == config_before
    assert [payload["messages"] for payload in payloads] == [first, second, [first[0]]]
    assert (first, second) == originals


@pytest.mark.asyncio
@pytest.mark.parametrize("transport_case", ["complete", "whole_user_removed", "sources_removed_receipt_kept"])
async def test_candidate_receipt_probe_blocks_whole_message_loss_but_not_partial_loss(
    monkeypatch, extraction_window, transport_case,
):
    """A protocol thought experiment; the mock echo is not an LLM quality test.

    This wrapper is test-only. Production schema/prompt version and adapters
    have not been changed to require the proposed receipt.
    """
    memory, pipeline, service, batch = extraction_window
    expected = hashlib.sha256(("input-receipt-probe.v1:" + batch.input_digest).encode()).hexdigest()
    payloads = []

    async def echo_surviving_receipt(request):
        payload = json.loads(request.content)
        payloads.append(payload)
        if transport_case == "whole_user_removed" and len(payloads) == 1:
            return error(400, "maximum context length exceeded")
        candidate = json.loads(no_change_completion().content)
        output = json.loads(candidate["choices"][0]["message"]["content"])
        users = [message for message in payload["messages"] if message["role"] == "user"]
        if users:
            output["window_receipt"] = json.loads(users[-1]["content"])["window_receipt"]
        candidate["choices"][0]["message"]["content"] = json.dumps(output)
        return httpx.Response(200, json=candidate)

    async with real_host(monkeypatch, echo_surviving_receipt) as (adapter, _):
        async def candidate_model(messages):
            wire = copy.deepcopy(messages)
            wire[0]["content"] += "\nProbe only: echo the user JSON window_receipt in every result."
            data = json.loads(wire[1]["content"])
            data["window_receipt"] = expected
            if transport_case == "sources_removed_receipt_kept":
                # Simulate another provider discarding part of the payload.
                # This is not the pinned host's whole-message pop_record path.
                data["sources"] = []
            wire[1]["content"] = json.dumps(data)
            assert expected not in wire[0]["content"]
            candidate = json.loads(await adapter.generate(wire, provider_id="synthetic-chat"))
            if candidate.pop("window_receipt", None) != expected:
                raise ValueError("candidate window receipt mismatch")
            return json.dumps(candidate)

        if transport_case == "whole_user_removed":
            with pytest.raises(ValueError, match="receipt mismatch"):
                await service.run(batch, candidate_model, model_id="synthetic-receipt-probe")
        else:
            await service.run(batch, candidate_model, model_id="synthetic-receipt-probe")

    if transport_case == "whole_user_removed":
        assert len(payloads) == 2
        assert all(message["role"] == "system" for message in payloads[1]["messages"])
        assert await memory.get_extraction_batch(batch.batch_id) is None
        assert await pipeline.consumed(batch.generation_id, source_ids(batch)) == set()
    else:
        assert len(payloads) == 1
        transmitted = json.loads(payloads[0]["messages"][-1]["content"])
        assert transmitted["window_receipt"] == expected
        assert bool(transmitted["sources"]) == (transport_case == "complete")
        # The partial-loss case still commits. Correct receipt echo proves only
        # that receipt survived, not that every fact reached the model.
        assert await memory.get_extraction_batch(batch.batch_id) is not None
        assert await pipeline.consumed(batch.generation_id, source_ids(batch)) == set(source_ids(batch))
