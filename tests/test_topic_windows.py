import asyncio

import pytest

from core.topic_windows import TopicPolicy, TopicWindowJudge, judge_vectors

TEXT = "A meaningful sentence about a conversation topic."


def decide(vectors, **kwargs):
    return judge_vectors([TEXT] * len(vectors), vectors, **kwargs)


def test_coherent_new_topic_requires_two_messages_and_enough_history():
    result = decide([[1, 0]] * 3 + [[0, 1]] * 2)
    assert result.trusted_switch and result.reason == "coherent_new_topic"
    assert result.cached_count == 5 and result.center_count == 1
    assert result.boundary_index == 3
    assert not decide([[1, 0]] * 3 + [[0, 1]]).trusted_switch
    assert not decide([[1, 0]] * 2 + [None] + [[0, 1]] * 2).trusted_switch


def test_parallel_topics_keep_separate_centers_and_returning_topic_is_not_switch():
    result = decide([[1, 0, 0], [0, 1, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0]])
    assert not result.trusted_switch and result.reason == "existing_topic"
    assert result.center_count == 2
    fresh = decide([[1, 0, 0], [0, 1, 0], [1, 0, 0], [0, 0, 1], [0, 0, 1]])
    assert fresh.trusted_switch and fresh.center_count == 2


def test_missing_short_incoherent_or_invalid_tail_is_conservative():
    assert decide([[1, 0]] * 3 + [None, [0, 1]]).reason == "uncached_recent_message"
    assert decide([[1, 0]] * 3 + [[1, 0], [0, 1]]).reason == "incoherent_recent_messages"
    assert decide([[1, 0]] * 3 + [[0, 0], [0, 1]]).reason == "uncached_recent_message"
    assert decide([[1, 0]] * 3 + [[float("nan"), 1], [0, 1]]).reason == "uncached_recent_message"
    assert decide([[1, 0]] * 3 + [[False, 1], [0, 1]]).reason == "uncached_recent_message"
    texts = [TEXT] * 4 + ["嗯，好的！"]
    assert judge_vectors(texts, [[1, 0]] * 3 + [[0, 1]] * 2).reason == "short_recent_message"


def test_no_forced_average_for_history_more_diverse_than_center_budget():
    policy = TopicPolicy(max_centers=2)
    result = decide([[1, 0, 0], [0, 1, 0], [0, 0, 1], [-1, 0, 0], [-1, 0, 0]], policy=policy)
    assert result.reason == "history_too_diverse" and not result.trusted_switch


def test_unscaled_vectors_normalize_and_dimension_changes_do_not_trigger():
    assert decide([[15, 0], [2, 0], [1, 0], [0, 5], [0, 3]]).trusted_switch
    assert decide([[1, 0]] * 3 + [[0, 1, 0]] * 2).reason == "inconsistent_dimensions"
    with pytest.raises(ValueError):
        judge_vectors([TEXT], [])


def test_sampling_uses_only_bounded_recent_messages():
    # Old, highly varied dimensions outside the bounded sample are ignored.
    policy = TopicPolicy(sample_limit=5)
    result = decide([[1, 0, 0]] * 100 + [[1, 0]] * 3 + [[0, 1]] * 2, policy=policy)
    assert result.trusted_switch and result.boundary_index == 103


def test_new_topic_can_have_more_than_two_messages_without_polluting_old_centers():
    result = decide([[1, 0]] * 4 + [[0, 1]] * 5)
    assert result.trusted_switch and result.boundary_index == 4
    assert result.center_count == 1


def test_short_replies_follow_previous_long_message_and_never_create_boundary():
    texts = [TEXT] * 3 + ["嗯"] + [TEXT, "好", TEXT, "了解", TEXT, "收到"]
    vectors = [[1, 0]] * 3 + [None] + [[0, 1], None, [0, 1], None, [0, 1], None]
    result = judge_vectors(texts, vectors)
    assert result.trusted_switch and result.boundary_index == 4
    assert result.cached_count == 6
    single = judge_vectors([TEXT] * 3 + [TEXT, "好", "知道了"], [[1, 0]] * 3 + [[0, 1], None, None])
    assert not single.trusted_switch and single.boundary_index is None


def test_uncached_long_message_inside_new_topic_cannot_be_bridged_like_short_reply():
    result = decide([[1, 0]] * 3 + [[0, 1], None, [0, 1], [0, 1]])
    assert not result.trusted_switch and result.reason == "uncached_recent_message"


@pytest.mark.parametrize("kwargs", [
    {"sample_limit": 4}, {"recent_messages": 1}, {"max_centers": 0}, {"min_text_chars": True},
    {"switch_similarity": 0.8}, {"cluster_similarity": 2}, {"cache_timeout_seconds": 0},
    {"cache_timeout_seconds": float("inf")}, {"cluster_similarity": False},
])
def test_policy_rejects_invalid_configuration(kwargs):
    with pytest.raises(ValueError):
        TopicPolicy(**kwargs)


class CachedOnly:
    def __init__(self, vectors=None, error=None, delay=0):
        self.vectors = vectors
        self.error = error
        self.delay = delay
        self.calls = []

    async def cached(self, texts, *, namespace):
        self.calls.append((texts, namespace))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error:
            raise self.error
        return self.vectors

    async def embed(self, *args, **kwargs):
        raise AssertionError("topic judgement must never request new vectors")


@pytest.mark.asyncio
async def test_async_judge_reads_cache_in_current_space_without_network():
    embeddings = CachedOnly([[1, 0]] * 3 + [[0, 1]] * 2)
    result = await TopicWindowJudge(embeddings).judge("space-a", [{"content_text": TEXT}] * 5)
    assert result.trusted_switch
    assert embeddings.calls == [([TEXT] * 5, "space-a")]


@pytest.mark.asyncio
async def test_async_boundary_maps_back_to_unsampled_input_with_short_replies():
    policy = TopicPolicy(sample_limit=9)
    embeddings = CachedOnly([[1, 0]] * 3 + [[0, 1]] * 3)
    texts = ["样本外"] * 40 + [TEXT] * 3 + [TEXT, "好", TEXT, "了解", TEXT, "收到"]
    result = await TopicWindowJudge(embeddings, policy=policy).judge("space", [{"content_text": text} for text in texts])
    assert result.trusted_switch and result.boundary_index == 43
    assert embeddings.calls == [([TEXT] * 6, "space")]


@pytest.mark.asyncio
async def test_short_tail_skips_cache_lookup_entirely():
    embeddings = CachedOnly(error=AssertionError())
    records = [{"content_text": TEXT}] * 4 + [{"content_text": "yes"}]
    result = await TopicWindowJudge(embeddings).judge("space", records)
    assert result.reason == "short_recent_message" and not embeddings.calls


@pytest.mark.asyncio
async def test_empty_older_message_is_skipped_in_cache_batch():
    embeddings = CachedOnly([[1, 0]] * 3 + [[0, 1]] * 2)
    records = [{"content_text": ""}] + [{"content_text": TEXT}] * 5
    result = await TopicWindowJudge(embeddings).judge("space", records)
    assert result.trusted_switch and len(embeddings.calls[0][0]) == 5


@pytest.mark.asyncio
async def test_cache_failure_or_timeout_is_only_absence_of_topic_evidence():
    records = [{"content_text": TEXT}] * 5
    result = await TopicWindowJudge(CachedOnly(error=RuntimeError("secret response"))).judge("space", records)
    assert result.reason == "cache_unavailable" and not result.trusted_switch
    result = await TopicWindowJudge(CachedOnly(delay=1), policy=TopicPolicy(cache_timeout_seconds=0.01)).judge("space", records)
    assert result.reason == "cache_unavailable" and not result.trusted_switch


@pytest.mark.asyncio
async def test_cancellation_propagates():
    with pytest.raises(asyncio.CancelledError):
        await TopicWindowJudge(CachedOnly(error=asyncio.CancelledError())).judge("space", [{"content_text": TEXT}] * 5)
