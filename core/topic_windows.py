"""Conservative topic-window hints using only already-cached message vectors."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from .embeddings import EmbeddingService


@dataclass(frozen=True, slots=True)
class TopicPolicy:
    sample_limit: int = 32
    recent_messages: int = 2
    min_history_vectors: int = 3
    min_text_chars: int = 12
    max_centers: int = 3
    cluster_similarity: float = 0.75
    switch_similarity: float = 0.45
    cache_timeout_seconds: float = 0.5

    def __post_init__(self) -> None:
        for name in ("sample_limit", "recent_messages", "min_history_vectors", "min_text_chars", "max_centers"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.recent_messages < 2 or self.sample_limit < self.recent_messages + self.min_history_vectors:
            raise ValueError("topic samples need at least two recent and enough historical messages")
        for name in ("cluster_similarity", "switch_similarity"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or not -1 <= value <= 1:
                raise ValueError(f"{name} must be a finite cosine threshold")
        if self.switch_similarity >= self.cluster_similarity:
            raise ValueError("topic switch threshold must be below cluster similarity")
        if (isinstance(self.cache_timeout_seconds, bool) or not isinstance(self.cache_timeout_seconds, (float, int))
                or not math.isfinite(self.cache_timeout_seconds) or self.cache_timeout_seconds <= 0):
            raise ValueError("cache timeout must be finite and positive")


@dataclass(frozen=True, slots=True)
class TopicDecision:
    trusted_switch: bool
    reason: str
    cached_count: int = 0
    center_count: int = 0
    boundary_index: int | None = None


def _unit(vector: Sequence[float]) -> list[float] | None:
    if not vector or any(isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value)
                         for value in vector):
        return None
    norm = math.sqrt(math.fsum(value * value for value in vector))
    return [value / norm for value in vector] if math.isfinite(norm) and norm > 0 else None


def _similarity(left: Sequence[float], right: Sequence[float]) -> float:
    return math.fsum(a * b for a, b in zip(left, right, strict=True))


def judge_vectors(texts: Sequence[str], vectors: Sequence[Sequence[float] | None], *,
                  policy: TopicPolicy = TopicPolicy()) -> TopicDecision:
    """返回连续新话题的起点；短回复随前面的长句，不单独作为切换证据。

    新话题可以已经持续多条，不能把它的前几条纳入旧中心后误判为旧话题。
    边界是相对完整输入的索引；样本以外的历史不参与相似度判断。
    """
    if len(texts) != len(vectors):
        raise ValueError("topic texts and vectors must have equal lengths")
    offset = max(0, len(texts) - policy.sample_limit)
    texts, vectors = texts[-policy.sample_limit:], vectors[-policy.sample_limit:]
    if len(texts) < policy.recent_messages + policy.min_history_vectors:
        return TopicDecision(False, "insufficient_messages")
    significant = [sum(character.isalnum() for character in text) >= policy.min_text_chars for text in texts]
    if sum(significant) < policy.recent_messages + policy.min_history_vectors:
        return TopicDecision(False, "short_recent_message")
    normalized = [_unit(vector) if vector is not None and usable else None
                  for vector, usable in zip(vectors, significant, strict=True)]
    present = [vector for vector in normalized if vector is not None]
    count = len(present)
    if len({len(vector) for vector in present}) > 1:
        return TopicDecision(False, "inconsistent_dimensions", count)
    # 只用连续且互相相近的长句定位尾部；中间短句继承此前的归属。
    recent = []
    boundary = len(texts)
    for index in reversed(range(len(texts))):
        if not significant[index]:
            continue
        vector = normalized[index]
        if vector is None:
            return TopicDecision(False, "uncached_recent_message", count)
        if any(_similarity(vector, member) < policy.cluster_similarity for member in recent):
            break
        recent.append(vector)
        boundary = index
    if len(recent) < policy.recent_messages:
        return TopicDecision(False, "incoherent_recent_messages", count)
    historical = [vector for vector in normalized[:boundary] if vector is not None]
    if len(historical) < policy.min_history_vectors:
        return TopicDecision(False, "insufficient_cached_history", count)
    centers: list[list[float]] = []
    populations: list[int] = []
    for vector in historical:
        similarities = [_similarity(vector, center) for center in centers]
        if similarities and max(similarities) >= policy.cluster_similarity:
            index = max(range(len(centers)), key=similarities.__getitem__)
            population = populations[index]
            center = _unit([old * population + new for old, new in zip(centers[index], vector, strict=True)])
            if center is None:
                return TopicDecision(False, "unstable_center", count, len(centers))
            centers[index] = center
            populations[index] += 1
        elif len(centers) < policy.max_centers:
            centers.append(list(vector))
            populations.append(1)
        else:
            return TopicDecision(False, "history_too_diverse", count, len(centers))
    if any(_similarity(vector, center) >= policy.switch_similarity for vector in recent for center in centers):
        return TopicDecision(False, "existing_topic", count, len(centers))
    return TopicDecision(True, "coherent_new_topic", count, len(centers), offset + boundary)


class TopicWindowJudge:
    def __init__(self, embeddings: EmbeddingService, *, policy: TopicPolicy = TopicPolicy()) -> None:
        self.embeddings = embeddings
        self.policy = policy

    async def judge(self, space_id: str, records: Sequence[Mapping[str, Any]]) -> TopicDecision:
        offset = max(0, len(records) - self.policy.sample_limit)
        texts = [str(record.get("content_text", "")) for record in records[-self.policy.sample_limit:]]
        if len(texts) < self.policy.recent_messages + self.policy.min_history_vectors:
            return TopicDecision(False, "insufficient_messages")
        significant = [index for index, text in enumerate(texts)
                       if sum(character.isalnum() for character in text) >= self.policy.min_text_chars]
        if len(significant) < self.policy.recent_messages + self.policy.min_history_vectors:
            return TopicDecision(False, "short_recent_message")
        try:
            async with asyncio.timeout(self.policy.cache_timeout_seconds):
                available = await self.embeddings.cached([texts[index] for index in significant], namespace=space_id)
                vectors: list[list[float] | None] = [None] * len(texts)
                for index, vector in zip(significant, available, strict=True):
                    vectors[index] = vector
                result = await asyncio.to_thread(judge_vectors, texts, vectors, policy=self.policy)
                return replace(result, boundary_index=result.boundary_index + offset) if result.trusted_switch else result
        except Exception:
            # A disposable cache or unavailable provider must never block source
            # capture or turn the absence of evidence into a trusted switch.
            return TopicDecision(False, "cache_unavailable")
