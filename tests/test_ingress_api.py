import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

pytest.importorskip("astrbot.api.web")

import adapters.ingress_api as module  # noqa: E402
from adapters.ingress_api import IngressAPI  # noqa: E402
from core.ingress import IngressService  # noqa: E402
from storage.ingress_store import IngressStore  # noqa: E402
from storage.source_store import SourceStore  # noqa: E402
from tests.test_source_store import make_source  # noqa: E402


@pytest_asyncio.fixture
async def api(tmp_path, monkeypatch):
    store = IngressStore(tmp_path / "ingress.db")
    sources = SourceStore(tmp_path / "source.db")
    await store.open()
    await sources.open()
    ingress = IngressService(store, sources, SimpleNamespace())
    plugin = SimpleNamespace(ready=True, terminating=False, ingress=ingress)
    plugin.volatile_retry_calls = 0

    async def retry_failed():
        plugin.volatile_retry_calls += 1
        return 2

    plugin.retry_failed_sources = retry_failed
    result = IngressAPI(plugin)
    result.test_body = {}
    result.test_query = {}

    async def body(**kwargs):
        return result.test_body

    monkeypatch.setattr(
        module, "request", SimpleNamespace(json=body, query=result.test_query)
    )
    try:
        yield result
    finally:
        await ingress.stop()
        await store.close()
        await sources.close()


def payload(response, status=200):
    assert response.status_code == status, response.body
    return json.loads(response.body)


def test_registration_uses_host_api_bridge(api):
    registrations = []
    api.register(
        SimpleNamespace(register_web_api=lambda *args: registrations.append(args))
    )
    assert {(row[0], tuple(row[2])) for row in registrations} == {
        ("/astrbot_plugin_neomemory/ingress", ("GET",)),
        ("/astrbot_plugin_neomemory/ingress/record", ("GET",)),
        ("/astrbot_plugin_neomemory/ingress/retry", ("POST",)),
    }
    assert all(callable(row[1]) for row in registrations)
    api.register(SimpleNamespace())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attribute,value", [("ready", False), ("terminating", True), ("ingress", None)]
)
async def test_all_endpoints_guard_unavailable_or_terminating_plugin(
    api, attribute, value
):
    setattr(api.plugin, attribute, value)
    for handler in (api.records, api.record, api.retry):
        assert (
            payload(await handler(), 503)["data"]["error_code"] == "ingress_unavailable"
        )


@pytest.mark.asyncio
async def test_list_paginates_without_raw_text_tokens_or_payload(api):
    ingress = api.plugin.ingress
    for i in range(3):
        await ingress.store.enqueue(make_source(str(i), "PRIVATE RAW SOURCE BODY"))
    await ingress.store.claim()
    api.test_query.update(limit="2")
    result = payload(await api.records())
    assert result["has_more"] and result["limit"] == 2
    assert result["health"]["pending_count"] == 3
    assert "PRIVATE RAW SOURCE BODY" not in json.dumps(result)
    for row in result["items"]:
        assert not {"source", "payload_json", "lease_token"} & set(row)
    api.test_query.update(offset="2")
    last = payload(await api.records())
    assert not last["has_more"] and len(last["items"]) == 1


@pytest.mark.asyncio
async def test_record_is_explicit_raw_detail_and_missing_is_404(api):
    source = make_source(content="private original")
    await api.plugin.ingress.store.enqueue(source)
    api.test_query["source_id"] = source.source_id
    detail = payload(await api.record())
    assert detail["source"]["content_text"] == "private original"
    assert detail["source_id"] == source.source_id
    assert "lease_token" not in detail
    api.test_query["source_id"] = "unknown"
    assert (
        payload(await api.record(), 404)["data"]["error_code"]
        == "ingress_record_not_found"
    )


@pytest.mark.asyncio
async def test_retry_one_only_resets_selected_record(api):
    source = make_source()
    ingress = api.plugin.ingress
    await ingress.store.enqueue(source)
    claim = await ingress.store.claim()
    await ingress.store.fail(
        claim, error_code="source_io_error", retryable=False, retry_seconds=1
    )
    api.test_body = {"source_id": source.source_id}
    result = payload(await api.retry())
    assert result["retried"] == 1 and result["readmitted"] == 0
    assert result["health"]["counts"]["queued"] == 1
    assert api.plugin.volatile_retry_calls == 0
    assert payload(await api.retry())["retried"] == 0
    api.test_body = {"source_id": "unknown"}
    payload(await api.retry(), 404)


@pytest.mark.asyncio
async def test_retry_all_readmits_volatile_before_durable_retry(api, monkeypatch):
    calls = []

    async def volatile():
        calls.append("volatile")
        return 3

    async def durable(source_id):
        calls.append(("durable", source_id))
        return 4

    api.plugin.retry_failed_sources = volatile
    monkeypatch.setattr(api.plugin.ingress, "retry", durable)
    result = payload(await api.retry())
    assert calls == ["volatile", ("durable", None)]
    assert result["readmitted"] == 3 and result["retried"] == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        None,
        [],
        "all",
        {"all": True},
        {"source_id": None},
        {"source_id": []},
        {"source_id": ""},
        {"source_id": "x" * 201},
    ],
)
async def test_retry_body_validation_precedes_any_mutation(api, body):
    api.test_body = body
    assert (
        payload(await api.retry(), 400)["data"]["error_code"]
        == "invalid_ingress_request"
    )
    assert api.plugin.volatile_retry_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query",
    [
        {"limit": "0"},
        {"limit": "101"},
        {"limit": True},
        {"offset": "-1"},
        {"offset": "1000001"},
        {"limit": "1.5"},
    ],
)
async def test_pagination_is_strict_and_bounded(api, query):
    api.test_query.update(query)
    payload(await api.records(), 400)


@pytest.mark.asyncio
async def test_exception_responses_never_expose_private_storage_errors(
    api, monkeypatch
):
    async def broken(**kwargs):
        raise RuntimeError("PRIVATE CONTENT token=secret database=/private/location")

    monkeypatch.setattr(api.plugin.ingress, "list", broken)
    result = payload(await api.records(), 503)
    assert result["data"]["error_code"] == "ingress_request_failed"
    assert "PRIVATE" not in json.dumps(result)
    assert "secret" not in json.dumps(result)
