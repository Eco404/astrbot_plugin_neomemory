"""Vectorized cache normalization preserves cosine inputs and cache validation."""

import numpy as np
import pytest

from adapters.providers import ProviderAdapter, normalize_vectors
from core.embeddings import EmbeddingService, _normalize_cached_vectors
from storage.cache_store import CacheStore, text_hash
from tests.test_providers import FakeContext


def test_cache_normalization_matches_reference_for_large_and_extreme_batches():
    samples = np.random.default_rng(17).standard_normal((100, 768)).tolist()
    samples.extend([[1e308] * 768, [5e-324] * 768])
    expected = normalize_vectors(samples, count=len(samples), dimension=768)
    actual = _normalize_cached_vectors([None, *samples, None], 768)
    assert actual[0] is actual[-1] is None
    np.testing.assert_allclose(actual[1:-1], expected, rtol=1e-14, atol=1e-16)
    assert samples[0] != actual[1]  # Raw coordinates were not mutated.
    assert _normalize_cached_vectors([None, None], 0) == [None, None]


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", ["[true,1]", '["1",2]', "[NaN,1]", "[Infinity,1]", "[0,0]", "[1]", "[[1],2]"])
async def test_fast_cached_normalization_never_bypasses_cache_integrity(tmp_path, encoded):
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    context = FakeContext()
    service = EmbeddingService(ProviderAdapter(context), cache, provider_id="embedding-test")
    try:
        await cache.put_vectors(service.fingerprint, "space", [text_hash("query")], [[1, 2]])
        await cache.database.execute("UPDATE embedding_cache SET vector_json=?", (encoded,))
        assert await service.cached(["query"], namespace="space") == [None]
        assert service.last_cache_error == "ValueError"
        assert context.embedding.calls == []
    finally:
        await cache.close()
