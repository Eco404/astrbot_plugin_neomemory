from dataclasses import dataclass

from core.identity import (
    default_logical_user_id,
    extract_event_identity,
    group_memory_space_id,
    private_memory_space_id,
)


@dataclass
class MessageObject:
    message_id: str = "message-1"
    message: list = None

    def __post_init__(self):
        if self.message is None:
            self.message = []


class FakeEvent:
    message_obj = MessageObject()

    def get_platform_id(self):
        return "adapter-1"

    def get_platform_name(self):
        return "aiocqhttp"

    def get_message_type(self):
        class MessageType:
            value = "FriendMessage"

        return MessageType()

    @property
    def unified_msg_origin(self):
        return "adapter-1:FriendMessage:user-1"

    def get_session_id(self):
        return "user-1"

    def get_group_id(self):
        return ""

    def get_sender_id(self):
        return "user-1"

    def get_sender_name(self):
        return "Alice"

    def get_self_id(self):
        return "bot-1"


def test_extract_event_identity_uses_public_event_fields():
    identity = extract_event_identity(FakeEvent())

    assert identity.platform_instance_id == "adapter-1"
    assert identity.platform_name == "aiocqhttp"
    assert identity.message_type == "FriendMessage"
    assert identity.account_id == "user-1"
    assert identity.message_id == "message-1"
    assert identity.stream_id.startswith("stream_")


def test_logical_user_and_scope_ids_are_stable():
    first = default_logical_user_id("adapter-1", "user-1")
    second = default_logical_user_id("adapter-1", "user-1")

    assert first == second
    assert private_memory_space_id("default", first) == f"persona:default:user:{first}"
    assert group_memory_space_id("default", "adapter-1", "group-1") == (
        "persona:default:platform:adapter-1:group:group-1"
    )
