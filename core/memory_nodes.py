"""Validation constants for derived memory nodes."""

from __future__ import annotations

from .text_lengths import USER_REFERENCE_MAX_CHARS, encoded_text_limit, validate_readable_text

NODE_TYPES = frozenset({"atomic", "event", "topic"})
NODE_STATUSES = frozenset(
    {"active", "historical", "disabled", "superseded", "deleted"}
)
EDGE_TYPES = frozenset({"contains", "related", "follows", "supersedes"})
PARTICIPANT_KINDS = frozenset({"logical_user", "account", "persona", "external"})
TITLE_MAX_CHARS = 60
TITLE_REFERENCE_MAX_CHARS = USER_REFERENCE_MAX_CHARS
TITLE_MAX_ENCODED_CHARS = encoded_text_limit(TITLE_MAX_CHARS)


def validate_title(title: str) -> str:
    """标题按可读长度校验，完整的稳定或短用户引用都只占一个字符。"""
    if not isinstance(title, str):
        raise ValueError("memory node title must be text")
    value = title.strip()
    if not value:
        raise ValueError("memory node title must not be empty")
    return validate_readable_text(value, TITLE_MAX_CHARS, label="memory node title")


def validate_node_type(node_type: str) -> str:
    value = str(node_type).strip().lower()
    if value not in NODE_TYPES:
        raise ValueError(f"unsupported node type: {node_type}")
    return value


def validate_node_status(status: str) -> str:
    value = str(status).strip().lower()
    if value not in NODE_STATUSES:
        raise ValueError(f"unsupported node status: {status}")
    return value


def validate_edge_type(edge_type: str) -> str:
    value = str(edge_type).strip().lower()
    if value not in EDGE_TYPES:
        raise ValueError(f"unsupported edge type: {edge_type}")
    return value


def validate_content(content_text: str, *, max_chars: int = 20000) -> str:
    value = str(content_text).strip()
    if not value:
        raise ValueError("memory node content must not be empty")
    if max_chars > 0:
        validate_readable_text(value, max_chars, label="memory node content")
    return value
