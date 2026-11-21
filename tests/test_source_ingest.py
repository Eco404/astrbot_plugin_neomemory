from dataclasses import dataclass

import pytest

from core.source_ingest import normalize_agent_response, normalize_event


@dataclass
class Component:
    text: str

    def model_dump(self, **kwargs):
        del kwargs
        return {"type": "plain", "text": self.text}


@dataclass
class MessageObject:
    message_id: str = "msg-1"
    message: list = None

    def __post_init__(self):
        if self.message is None:
            self.message = [Component("component text")]


class FakeEvent:
    message_obj = MessageObject()
    message_str = "hello"
    unified_msg_origin = "adapter:FriendMessage:user"

    def get_platform_id(self):
        return "adapter"

    def get_platform_name(self):
        return "adapter"

    def get_message_type(self):
        class MessageType:
            value = "FriendMessage"

        return MessageType()

    def get_session_id(self):
        return "user"

    def get_group_id(self):
        return ""

    def get_sender_id(self):
        return "user"

    def get_sender_name(self):
        return "Alice"

    def get_self_id(self):
        return "bot"

    def get_message_str(self):
        return self.message_str

    def get_extra(self, key, default=None):
        return {"persona_id": "persona-lumi"}.get(key, default)


class FakeResponse:
    role = "assistant"
    id = "response-1"
    is_chunk = False
    completion_text = "好的，我记住了。"


def test_normalize_event_preserves_component_json_and_dedup_key():
    source = normalize_event(FakeEvent(), observed_at=123.0)

    assert source is not None
    assert source.content_text == "hello"
    assert "component text" in (source.content_json or "")
    assert source.dedup_key.endswith(":msg-1")
    assert source.source_id.startswith("src_")
    assert source.observed_at == 123.0


def test_normalize_event_records_explicit_truncation():
    source = normalize_event(FakeEvent(), max_content_chars=3, observed_at=123.0)

    assert source is not None
    assert source.content_text == "hel"
    assert source.metadata["content_truncated"] is True
    assert source.metadata["original_content_length"] == 5


def test_normalize_event_keeps_persona_snapshot_and_pending_flag():
    source = normalize_event(FakeEvent(), observed_at=123.0)

    assert source is not None
    assert source.persona_id == "persona-lumi"
    assert source.identity_pending is False


def test_normalize_event_marks_missing_persona_as_pending():
    class NoPersonaEvent(FakeEvent):
        def get_extra(self, key, default=None):
            del key
            return default

    source = normalize_event(NoPersonaEvent(), observed_at=123.0)

    assert source is not None
    assert source.persona_id is None
    assert source.identity_pending is True


def test_normalize_agent_response_ignores_chunks_and_keeps_response_id():
    event = FakeEvent()
    source = normalize_agent_response(event, FakeResponse(), observed_at=124.0)

    assert source is not None
    assert source.actor_kind == "agent"
    assert source.response_id == "response-1"
    assert source.observed_at == 124.0

    chunk = FakeResponse()
    chunk.is_chunk = True
    assert normalize_agent_response(event, chunk) is None


@pytest.mark.parametrize("agent", [False, True])
def test_host_conversation_metadata_does_not_change_stable_source_identity(agent):
    def normalized(**kwargs):
        if agent:
            return normalize_agent_response(FakeEvent(), FakeResponse(), observed_at=123.0, **kwargs)
        return normalize_event(FakeEvent(), observed_at=123.0, **kwargs)

    original = normalized()
    empty = normalized(host_conversation_id="")
    current = normalized(host_conversation_id="conversation-current")
    another = normalized(host_conversation_id="conversation-another")
    assert "host_conversation_id" not in original.metadata
    assert "host_conversation_id" not in empty.metadata
    assert current.metadata["host_conversation_id"] == "conversation-current"
    assert another.metadata["host_conversation_id"] == "conversation-another"
    assert original.source_id == empty.source_id == current.source_id == another.source_id
    assert original.dedup_key == current.dedup_key
    assert original.persona_id == current.persona_id == "persona-lumi"


@pytest.mark.parametrize("agent", [False, True])
@pytest.mark.parametrize("explicit_persona", [None, "resolved-host-persona"])
def test_host_conversation_preserves_explicit_persona_resolution_policy(agent, explicit_persona):
    kwargs = {
        "persona_id": explicit_persona, "resolve_event_persona": False,
        "host_conversation_id": "conversation-current", "observed_at": 123.0,
    }
    if agent:
        source = normalize_agent_response(FakeEvent(), FakeResponse(), **kwargs)
    else:
        source = normalize_event(FakeEvent(), **kwargs)
    assert source.persona_id == explicit_persona
    assert source.identity_pending is (explicit_persona is None)
    assert source.metadata["host_conversation_id"] == "conversation-current"


def test_group_capture_preserves_host_components_and_direct_address():
    from astrbot.core.message.components import At, AtAll, Plain, Reply
    import json

    class GroupEvent(FakeEvent):
        def get_message_type(self):
            return "GroupMessage"

    event = GroupEvent()
    event.message_obj = MessageObject(message=[
        At(qq="member-5", name="乙"), At(qq="bot"), AtAll(),
        Reply(id="quoted", sender_id="member-8", sender_nickname="丙"), Plain("资料到了。"),
    ])
    source = normalize_event(event)
    components = json.loads(source.content_json)
    assert components[0]["qq"] == "member-5"
    assert components[-1]["text"] == "资料到了。"
    assert components[-2]["sender_id"] == "member-8"
    assert source.metadata["group_addressing"] == [
        {"kind": "mention", "account_id": "member-5", "display_name": "乙"},
        {"kind": "mention", "account_id": "bot", "is_self": True},
        {"kind": "mention_all"},
        {"kind": "reply", "account_id": "member-8", "display_name": "丙", "message_id": "quoted"},
    ]
    assert "group_addressing" not in normalize_agent_response(event, FakeResponse()).metadata


def test_qq_group_uses_original_mention_identity_and_coalesces_bot_component():
    from types import SimpleNamespace

    class GroupEvent(FakeEvent):
        def get_message_type(self):
            return "GroupMessage"

        def get_platform_name(self):
            return "qq_official"

    event = GroupEvent()
    event.message_obj = MessageObject(message=[
        {"type": "at", "data": {"qq": "bot-marker"}},
        {"type": "reply", "data": {"id": "prev"}},
        {"type": "plain", "text": "<@someone> 请发一下链接。"},
    ])
    event.message_obj.raw_message = SimpleNamespace(raw_data={"mentions": [
        {"id": "someone", "member_openid": "account-6", "username": "乙"},
        {"id": "bot-marker", "is_you": True},
    ]})
    assert normalize_event(event).metadata["group_addressing"] == [
        {"kind": "mention", "account_id": "account-6", "mention_id": "someone", "display_name": "乙"},
        {"kind": "mention", "account_id": "bot-marker", "is_self": True},
        {"kind": "reply", "message_id": "prev"},
    ]
    event.message_obj.raw_message = SimpleNamespace(channel_id="channel")
    event.message_obj.message = [{"type": "at", "data": {"qq": "qq_official"}}]
    assert "group_addressing" not in normalize_event(event).metadata


def test_sync_capture_never_calls_an_async_wire_serializer():
    from unittest.mock import AsyncMock
    from types import SimpleNamespace
    import json

    wire = AsyncMock()
    event = FakeEvent()
    event.message_obj = MessageObject(message=[
        SimpleNamespace(dict=lambda: {"type": "plain", "text": "body"}, to_dict=wire)
    ])
    assert json.loads(normalize_event(event).content_json) == [{"type": "plain", "text": "body"}]
    wire.assert_not_called()
