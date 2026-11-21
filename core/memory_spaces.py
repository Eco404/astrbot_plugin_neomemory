"""Memory-space identity and validation helpers."""

from __future__ import annotations

from dataclasses import dataclass

SPACE_TYPES = frozenset({"private", "group"})


class RetiredIdentity(ValueError):
    """A removed identity cannot acquire a new derived-memory space."""


@dataclass(frozen=True, slots=True)
class MemorySpaceSpec:
    """Stable scope fields for a derived-memory space."""

    space_type: str
    persona_id: str
    logical_user_id: str | None = None
    platform_instance_id: str | None = None
    group_id: str | None = None

    def __post_init__(self) -> None:
        if self.space_type not in SPACE_TYPES:
            raise ValueError(f"unsupported memory space type: {self.space_type}")
        if not self.persona_id.strip():
            raise ValueError("persona_id is required for a memory space")
        if self.space_type == "private" and not (self.logical_user_id or "").strip():
            raise ValueError("private memory spaces require logical_user_id")
        if self.space_type == "group":
            if not (self.platform_instance_id or "").strip():
                raise ValueError("group memory spaces require platform_instance_id")
            if not (self.group_id or "").strip():
                raise ValueError("group memory spaces require group_id")

    @property
    def scope_key(self) -> str:
        if self.space_type == "private":
            return f"persona:{self.persona_id}:user:{self.logical_user_id}"
        return (
            f"persona:{self.persona_id}:platform:{self.platform_instance_id}"
            f":group:{self.group_id}"
        )


def private_space(persona_id: str, logical_user_id: str) -> MemorySpaceSpec:
    return MemorySpaceSpec("private", persona_id, logical_user_id=logical_user_id)


def group_space(
    persona_id: str, platform_instance_id: str, group_id: str
) -> MemorySpaceSpec:
    return MemorySpaceSpec(
        "group",
        persona_id,
        platform_instance_id=platform_instance_id,
        group_id=group_id,
    )
