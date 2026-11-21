import pytest

from core.memory_spaces import MemorySpaceSpec, group_space, private_space


def test_private_scope_key_is_persona_and_logical_user():
    spec = private_space("persona-a", "logical-alice")
    assert spec.scope_key == "persona:persona-a:user:logical-alice"
    assert spec.space_type == "private"


def test_group_scope_key_keeps_platform_instance():
    spec = group_space("persona-a", "qq-main", "group-1")
    assert spec.scope_key == "persona:persona-a:platform:qq-main:group:group-1"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"space_type": "private", "persona_id": "p"},
        {"space_type": "group", "persona_id": "p", "group_id": "g"},
        {"space_type": "other", "persona_id": "p"},
    ],
)
def test_space_spec_rejects_incomplete_scope(kwargs):
    with pytest.raises(ValueError):
        MemorySpaceSpec(**kwargs)
