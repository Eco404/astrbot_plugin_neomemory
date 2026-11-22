from types import SimpleNamespace
import pytest
from adapters.chat_model_capture import requested_chat_route, observed_chat_route


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_request_route_remains_distinct_from_response_model(explicit):
    provider = SimpleNamespace(meta=lambda: SimpleNamespace(id="chat"), get_model=lambda: "configured")
    async def lookup(umo):
        assert umo == "platform:FriendMessage:1"
        return provider
    context = SimpleNamespace(get_using_provider_async=lookup, get_provider_by_id=lambda key: provider)
    extra = {"selected_provider": "chosen"} if explicit else {}
    event = SimpleNamespace(get_extra=extra.get, unified_msg_origin="platform:FriendMessage:1")
    event._neomemory_chat_route = await requested_chat_route(context, event, SimpleNamespace(model="requested"))
    result = observed_chat_route(event, SimpleNamespace(raw_completion={"model": "reported"}))
    assert result == {"provider_id": "chosen" if explicit else "chat", "requested_model": "requested", "model_override": "requested",
                      "basis": "request_provider" if explicit else "session_config", "reported_model": "reported"}
    assert observed_chat_route(SimpleNamespace(), SimpleNamespace(raw_completion={"model":"reported"})) == {"reported_model":"reported"}
