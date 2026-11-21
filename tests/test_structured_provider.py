"""Real host Responses payloads use isolated clients and preserve shared config."""
import asyncio
import copy
import json

import httpx
import pytest

from adapters.providers import ProviderAdapter, StructuredOutputUnsupportedError
from tests.test_providers import MESSAGES, FakeContext

SCHEMA = {"type": "object", "properties": {"result": {"type": "string"}},
          "required": ["result"], "additionalProperties": False}


@pytest.fixture
def responses(monkeypatch):
    from astrbot.core.provider.sources.openai_responses_source import (
        ProviderOpenAIResponses,
    )
    clients, requests = [], []
    control = {"gate": None}
    async def respond(request):
        requests.append(json.loads(request.content))
        if control["gate"] is not None:
            await control["gate"].wait()
        if "failure" in control:
            return httpx.Response(400, json={"error": control["failure"]})
        return httpx.Response(200, json={
            "id": "resp_synthetic", "object": "response", "created_at": 1,
            "model": "synthetic", "status": "completed",
            "output": [{"type": "message", "id": "msg_synthetic", "role": "assistant",
                        "status": "completed", "content": [{"type": "output_text",
                        "text": '{"result":"ok"}', "annotations": []}]}],
            "usage": {"input_tokens": 12, "output_tokens": 4, "total_tokens": 16}})
    def client(self, config):
        created = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        clients.append(created)
        return created
    monkeypatch.setattr(ProviderOpenAIResponses, "_create_http_client", client)
    selected = ProviderOpenAIResponses({"id": "chat-test", "model": "synthetic",
        "type": "openai_responses", "key": ["synthetic"], "api_base": "https://example.test/v1",
        "custom_extra_body": {"text": {"verbosity": "low"}}}, {})
    context = FakeContext(); context.chat = selected
    return selected, ProviderAdapter(context), clients, requests, control


@pytest.mark.asyncio
async def test_structured_modes_reach_real_host_transport_without_mutating_shared_provider(responses):
    selected, adapter, clients, requests, _ = responses
    selected.set_model("selected-synthetic")
    original = copy.deepcopy(selected.provider_config)
    diagnostics = {}
    try:
        values = await asyncio.gather(
            adapter.generate(MESSAGES, provider_id="chat-test", output_format="json_object"),
            adapter.generate(MESSAGES, provider_id="chat-test", output_format="json_schema",
                             output_schema=SCHEMA, diagnostics=diagnostics))
        assert values == ['{"result":"ok"}'] * 2
        assert selected.provider_config == original
        assert not clients[0].is_closed
        assert all(client.is_closed for client in clients[1:])
        by_mode = {body["text"]["format"]["type"]: body for body in requests}
        assert all(body["model"] == "selected-synthetic" for body in requests)
        assert by_mode["json_schema"]["text"]["format"]["strict"] is True
        assert by_mode["json_schema"]["text"]["format"]["schema"] == SCHEMA
        assert by_mode["json_object"]["text"]["verbosity"] == "low"
        assert all([item["role"] for item in body["input"]] == ["system", "user"] for body in requests)
        assert diagnostics["response_status"] == "completed"
        assert diagnostics["response_id"] == "resp_synthetic"
        assert diagnostics["input_tokens"] == 12 and diagnostics["output_tokens"] == 4
        assert "content" not in diagnostics and "schema" not in diagnostics
    finally:
        await selected.terminate()


@pytest.mark.asyncio
async def test_structured_cancellation_closes_only_owned_client(responses):
    selected, adapter, clients, _, control = responses
    control["gate"] = asyncio.Event()
    try:
        with pytest.raises(TimeoutError):
            await adapter.generate(MESSAGES, provider_id="chat-test", output_format="json_object",
                                   timeout_seconds=.02)
        assert clients[1].is_closed and not clients[0].is_closed
    finally:
        await selected.terminate()


@pytest.mark.asyncio
async def test_unsupported_provider_does_not_silently_fall_back_to_text():
    context = FakeContext()
    with pytest.raises(StructuredOutputUnsupportedError):
        await ProviderAdapter(context).generate(MESSAGES, provider_id="chat-test", output_format="json_object")
    assert context.calls == []


@pytest.mark.asyncio
async def test_schema_uses_text_for_non_responses_provider():
    context = FakeContext()
    diagnostics = {}
    result = await ProviderAdapter(context).generate(
        MESSAGES, provider_id="chat-test", output_format="json_schema",
        output_schema=SCHEMA, diagnostics=diagnostics)
    assert result == context.chat_response
    assert context.calls == [{"chat_provider_id": "chat-test", "contexts": MESSAGES}]
    assert diagnostics["requested_output_format"] == "json_schema"
    assert diagnostics["output_format"] == "text"


@pytest.mark.asyncio
@pytest.mark.parametrize("code,param,message,fallback", [
    ("unsupported_value", "text.format.type", "Unsupported value", True),
    (None, None, "This model does not support structured outputs", True),
    ("invalid_json_schema", "text.format.schema", "Invalid schema: unsupported field", False),
    ("invalid_request_error", "model", "Unknown model", False),
])
async def test_schema_negotiation_after_server_rejection(responses, code, param, message, fallback):
    from openai import BadRequestError

    selected, adapter, clients, requests, control = responses
    control["failure"] = {"code": code, "param": param, "message": message}
    try:
        request = adapter.generate(MESSAGES, provider_id="chat-test",
                                   output_format="json_schema", output_schema=SCHEMA)
        if fallback:
            assert await request == adapter.context.chat_response
            assert adapter.context.calls == [{"chat_provider_id": "chat-test", "contexts": MESSAGES}]
        else:
            with pytest.raises(BadRequestError):
                await request
            assert adapter.context.calls == []
        assert len(requests) == 1 and clients[1].is_closed
    finally:
        await selected.terminate()


def _image_messages():
    return [MESSAGES[0], {"role": "user", "content": [
        {"type": "text", "text": "source s1 slot 1"},
        {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,Zmlyc3Q="}},
        {"type": "text", "text": "source s2 slot 1"},
        {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,c2Vjb25k"}},
    ]}]


@pytest.mark.asyncio
async def test_private_images_reach_real_responses_request_in_order(responses):
    selected, adapter, _, requests, _ = responses
    try:
        await adapter.generate(_image_messages(), provider_id="chat-test", output_format="json_schema", output_schema=SCHEMA)
        parts = requests[0]["input"][1]["content"]
        assert [p["type"] for p in parts] == ["input_text", "input_image", "input_text", "input_image"]
        assert parts[0]["text"] == "source s1 slot 1" and parts[3]["image_url"].endswith("c2Vjb25k")
    finally:
        await selected.terminate()


@pytest.mark.asyncio
async def test_private_vision_negotiation_respects_model_declaration_and_server(responses):
    from adapters.providers import VisionUnsupportedError

    selected, adapter, _, requests, control = responses
    try:
        selected.provider_config["modalities"] = ["text", "tool_use"]
        with pytest.raises(VisionUnsupportedError):
            await adapter.generate(_image_messages(), provider_id="chat-test")
        assert not requests and not adapter.context.calls
        selected.provider_config["modalities"] = ["text", "image"]
        control["failure"] = {"message": "This model does not support image input", "code": "invalid_request_error"}
        with pytest.raises(VisionUnsupportedError):
            await adapter.generate(_image_messages(), provider_id="chat-test", output_format="json_schema", output_schema=SCHEMA)
        assert len(requests) == 1
    finally:
        await selected.terminate()
