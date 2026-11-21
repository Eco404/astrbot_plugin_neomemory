"""Opt-in real API smoke test; only synthetic inputs and metadata are retained.

Run with Conda astrbot, adding the AstrBot v4.29.3 source and this plugin root to
PYTHONPATH. Credentials default to the gitignored docs/api_key.md; raw responses,
URLs, account identifiers, credentials and exception messages are never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import io
import json
import logging
import math
import os
import re
import tempfile
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo


def credentials(path: Path) -> dict[str, dict[str, str]]:
    sections: dict[str, dict[str, str]] = {}
    section: dict[str, str] | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            section = {}
            sections[line[3:].strip()] = section
        elif section is not None and re.match(r"^[a-z_]+:", line):
            key, value = line.split(":", 1)
            section[key.strip()] = value.strip()
    if not all(key in sections for key in ("ecoapi", "Cloudflare Embedding")):
        raise ValueError("required credential sections are unavailable")
    return sections


def failure(exc: Exception) -> dict:
    # str(exc) and repr(exc) can include HTTP responses, URLs or credentials.
    result = {"ok": False, "error_type": type(exc).__name__}
    status = getattr(exc, "status_code", None)
    if type(status) is int:
        result["http_status"] = status
    if hasattr(exc, "errors"):
        result["validation_errors"] = [
            {"type": item["type"], "loc": list(item["loc"])}
            for item in exc.errors(include_input=False)
        ]
    return result


async def run_checks(config: dict, runtime: Path, proxy: str, *, skip_llm: bool = False) -> dict:
    # These are real host classes, not replacement network implementations.
    from astrbot import __version__ as host_version
    from astrbot.core.provider.sources.openai_embedding_source import (
        OpenAIEmbeddingProvider,
    )
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
    from astrbot.core.star.context import Context

    from adapters.providers import ProviderAdapter
    from core.embeddings import EmbeddingService
    from core.extraction_schema import parse_extraction_output
    from core.prompts import build_extraction_prompt
    from storage.cache_store import CacheStore

    llm_config, embedding_config = config["ecoapi"], config["Cloudflare Embedding"]
    llm = ProviderOpenAIOfficial({
        "id": "isolated-llm", "type": "openai_chat_completion",
        "model": llm_config["model"], "api_base": llm_config["base_url"],
        "key": [llm_config["api_token"]], "timeout": 45,
    }, {})
    embedding = OpenAIEmbeddingProvider({
        "id": "isolated-embedding", "type": "openai_embedding",
        "embedding_model": embedding_config["model"],
        "embedding_api_base": (
            embedding_config["api_address"].rstrip("/")
            + "/accounts/" + embedding_config["account_id"] + "/ai/v1"
        ),
        "embedding_api_key": embedding_config["api_token"],
        "embedding_dimensions": 0, "embedding_dimensions_mode": "never",
        "timeout": 20, "proxy": proxy,
    }, {})
    counts = {"llm": 0, "embedding": 0}

    async def count_llm(_request):
        counts["llm"] += 1

    async def count_embedding(_request):
        counts["embedding"] += 1

    llm.client._client.event_hooks["request"].append(count_llm)
    embedding.client._client.event_hooks["request"].append(count_embedding)
    providers = {"isolated-llm": llm, "isolated-embedding": embedding}

    async def get_provider_by_id(provider_id):
        return providers.get(provider_id)

    # Reuse the real Context lookup and llm_generate methods. The tiny registry
    # replaces lifecycle construction only; it never substitutes a model call.
    context = object.__new__(Context)
    context.provider_manager = SimpleNamespace(
        inst_map=providers, provider_insts=[llm], embedding_provider_insts=[embedding],
        get_provider_by_id=get_provider_by_id,
    )
    adapter = ProviderAdapter(context, llm_concurrency=2, embedding_concurrency=2)
    cache = CacheStore(runtime / "cache.db")
    await cache.open()
    service = EmbeddingService(adapter, cache, provider_id="isolated-embedding", batch_size=2)
    result = {
        "tested_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
        "host_version": host_version,
        "host_source": importlib.import_module("astrbot").__file__,
        "llm_model": llm_config["model"], "embedding_model": embedding_config["model"],
        "host_llm_class": type(llm).__name__, "host_embedding_class": type(embedding).__name__,
        "embedding_route": "Cloudflare OpenAI-compatible /ai/v1/embeddings",
        "proxy_enabled_for_embedding": bool(proxy), "checks": [],
    }
    try:
        texts = ["合成测试用户喜欢摄影。", "合成测试用户正在学习钢琴。", "合成测试用户报名了摄影课程。"]
        before = counts["embedding"]
        started = time.perf_counter()
        try:
            vectors = await service.embed(
                [*texts, texts[0]], namespace="synthetic-space", timeout_seconds=30,
            )
            check = {
                "name": "embedding_batch_dedup", "ok": True,
                "input_count": 4, "unique_count": 3, "batch_size": 2,
                "output_count": len(vectors), "dimensions": sorted({len(v) for v in vectors}),
                "normalized": all(abs(math.fsum(x * x for x in v) - 1) < 1e-6 for v in vectors),
                "duplicate_equal": vectors[0] == vectors[-1],
                "learned_cache_dimension": await cache.get_dimension(service.fingerprint),
            }
        except Exception as exc:
            check = {"name": "embedding_batch_dedup", **failure(exc)}
        check.update(seconds=round(time.perf_counter() - started, 4), http_requests=counts["embedding"] - before)
        result["checks"].append(check)
        if check["ok"]:
            before = counts["embedding"]
            started = time.perf_counter()
            await cache.close()
            await cache.open()
            service = EmbeddingService(adapter, cache, provider_id="isolated-embedding", batch_size=2)
            try:
                warm = await service.embed([*texts, texts[0]], namespace="synthetic-space", timeout_seconds=5)
                error = max(
                    abs(left - right)
                    for actual, expected in zip(warm, vectors, strict=True)
                    for left, right in zip(actual, expected, strict=True)
                )
                # Cache reads normalize again; the semantic equality criterion
                # is a tight numeric tolerance, not float bitwise equality.
                check = {
                    "name": "embedding_cache_reopen", "ok": error < 1e-12,
                    "maximum_coordinate_error": error,
                }
            except Exception as exc:
                check = {"name": "embedding_cache_reopen", **failure(exc)}
            check.update(seconds=round(time.perf_counter() - started, 4), http_requests=counts["embedding"] - before)
            result["checks"].append(check)

        # A real host HTTP operation is cancelled under an intentionally tiny
        # outer deadline. It is not a demonstration of a remote server timeout.
        before = counts["embedding"]
        started = time.perf_counter()
        try:
            await adapter.embed_batch(
                ["合成截止时间测试，不写入缓存。"], provider_id="isolated-embedding", timeout_seconds=0.001,
            )
            check = {"name": "embedding_outer_deadline", "ok": False, "reason": "completed_before_deadline"}
        except TimeoutError:
            check = {"name": "embedding_outer_deadline", "ok": True, "error_type": "TimeoutError"}
        except Exception as exc:
            check = {"name": "embedding_outer_deadline", **failure(exc)}
        check.update(seconds=round(time.perf_counter() - started, 4), deadline_seconds=0.001,
                     http_requests=counts["embedding"] - before)
        result["checks"].append(check)

        known = [
            {"node_id": "node_course", "node_type": "event", "content_version": 1,
             "content_text": "合成测试用户报名了半年的摄影课程，正在准备首次外拍。",
             "importance": 0.6, "status": "active", "manual_protected": False},
            {"node_id": "node_weekend", "node_type": "atomic", "content_version": 1,
             "content_text": "合成测试用户的摄影课程首次外拍计划在周六。",
             "importance": 0.4, "status": "active", "manual_protected": False},
        ]
        cases = [
            ("smalltalk", "早安，好的。", []),
            ("event_update", "我报名的半年摄影课程已经结业，也完成了首次外拍；那次外拍确实在周六进行。", known),
        ]
        if skip_llm:
            cases = []
        for name, content, known_nodes in cases:
            messages = build_extraction_prompt(
                persona="合成测试人格：关心用户长期学习进展，记录有根据的经历。",
                sources=[{"source_id": "synthetic_source", "actor_kind": "user",
                          "actor_logical_user_id": "synthetic_user", "content_text": content}],
                known_nodes=known_nodes,
                space={"space_type": "private", "persona_id": "synthetic_persona", "logical_user_id": "synthetic_user"},
            )
            before = counts["llm"]
            started = time.perf_counter()
            try:
                raw = await adapter.generate(messages, provider_id="isolated-llm", timeout_seconds=60)
                parsed = parse_extraction_output(raw)
                check = {
                    "name": name, "ok": True, "schema_version": parsed.schema_version,
                    "result": parsed.result, "node_count": len(parsed.node_changes),
                    "update_count": sum(change.operation == "update" for change in parsed.node_changes),
                    "coverage_count": len(parsed.coverage_changes),
                    "profile_count": len(parsed.profile_changes), "output_characters": len(raw),
                    "coverage_kinds": [item.coverage_kind for item in parsed.coverage_changes],
                }
                del raw
            except Exception as exc:
                check = {"name": name, **failure(exc)}
            check.update(seconds=round(time.perf_counter() - started, 4), http_requests=counts["llm"] - before)
            result["checks"].append(check)
    finally:
        await cache.close()
        await llm.terminate()
        await embedding.terminate()
    result["http_requests_total"] = counts
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    parser.add_argument("--embedding-proxy", default="", help="Optional proxy for the Cloudflare host provider.")
    parser.add_argument("--skip-llm", action="store_true", help="Repeat only embedding/cache/deadline checks.")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    # Host modules can emit logs at import time. Never forward stdout/stderr
    # from host/provider execution: error bodies can contain sensitive data.
    try:
        with tempfile.TemporaryDirectory(prefix="neomemory-stage4-api-") as directory:
            runtime = Path(directory)
            (runtime / "data/config").mkdir(parents=True)
            os.environ["ASTRBOT_ROOT"] = str(runtime)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                from loguru import logger
                logger.remove()
                result = asyncio.run(run_checks(
                    credentials(args.credentials), runtime, args.embedding_proxy,
                    skip_llm=args.skip_llm,
                ))
    except Exception as exc:
        result = {"setup": failure(exc)}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    checks = result.get("checks", [])
    return 0 if checks and all(item["ok"] for item in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
