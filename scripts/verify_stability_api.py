"""Bounded opt-in real-provider replay of synthetic stability scenarios.

Uses the production durable extraction and vector retrieval paths. Standard
output holds only aggregates. Explicit evidence mode retains synthetic inputs
and before/after values in a fresh local directory for separate offline review.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

try:
    from .profile_candidate_diagnostics import CandidateRecorder, diagnose_profiles
    from .profile_evidence import EvidenceWriter
    from .profile_quality_cases import (
        preference_requirements,
        qualifier_cases,
        regression_requirements,
        selection_cases,
    )
    from .verify_stage4_api import credentials, failure
    from .verify_stage5_api import safe_job, wait_terminal
except ImportError:
    from profile_candidate_diagnostics import CandidateRecorder, diagnose_profiles
    from profile_evidence import EvidenceWriter
    from profile_quality_cases import (
        preference_requirements,
        qualifier_cases,
        regression_requirements,
        selection_cases,
    )
    from verify_stage4_api import credentials, failure
    from verify_stage5_api import safe_job, wait_terminal


QUALITY_MODES = {"quality", "preference-quality", "qualifiers", "selection"}


class HttpBudget:
    """Count SDK-internal requests too; reject before sending beyond a limit."""

    def __init__(self, total=8, *, llm_limit=6, embedding_limit=2):
        if type(total) is not int or not 1 <= total <= 8:
            raise ValueError("HTTP budget must be between 1 and 8")
        self.total = total
        self.counts = {"llm": 0, "embedding": 0}
        self.limits = {"llm": llm_limit, "embedding": embedding_limit}
        if any(type(value) is not int or not 0 <= value <= 8 for value in self.limits.values()):
            raise ValueError("invalid per-provider HTTP limit")

    def hook(self, kind):
        if kind not in self.counts:
            raise ValueError("unknown provider kind")

        async def count(_request):
            if sum(self.counts.values()) >= self.total or self.counts[kind] >= self.limits[kind]:
                raise RuntimeError("synthetic_replay_http_budget_exceeded")
            self.counts[kind] += 1

        return count


def finish(check, assertions):
    """Keep transport/structure failures separate from semantic observations."""
    check["assertions"] = assertions if check.get("structure_ok") else {}
    check["screening_status"] = (
        "passed" if assertions and all(assertions.values()) else "failed"
    ) if check.get("structure_ok") else "not_evaluated"
    check["semantic_status"] = (
        "needs_review" if check.get("review_required") and check.get("structure_ok")
        else check["screening_status"]
    )
    check["ok"] = check["semantic_status"] == "passed"


def skipped(name, reason):
    return {"name": name, "ok": False, "structure_ok": False,
            "semantic_status": "not_evaluated", "reason": reason, "http_requests": 0}


def profile_text(profiles, user_id):
    return " ".join(item["description"] + " " + " ".join(item["tags"])
                    for item in profiles if item["logical_user_id"] == user_id)


def continue_preference_chain(check):
    """Semantic screening never blocks the next structurally committed window."""
    return check.get("structure_ok") is True


def record_consumption(check, source_ids, consumed):
    """A successful prefix task is not a structurally complete test window."""
    check["all_sources_consumed"] = bool(source_ids) and set(source_ids) == set(consumed)
    if check.get("structure_ok") and not check["all_sources_consumed"]:
        check["structure_ok"] = False
        check["reason"] = "source_window_incomplete"


def evidence_snapshot(profiles, nodes, relationships):
    """Whitelist separate facts; never export provider/config/runtime objects."""
    def select(items, fields):
        return [{key: item[key] for key in fields if key in item} for item in items if item]

    return {
        "profiles": select(profiles, ("logical_user_id", "content_version", "description", "tags")),
        "nodes": select(nodes, ("node_id", "node_type", "content_version", "content_text", "status")),
        "relationships": select(relationships, ("logical_user_id", "content_version", "state_text")),
    }


def preference_cases(variant="standard"):
    initial = ("preference_initial", "黑咖啡", [
        "从去年开始我每天只喝黑咖啡，这是稳定的饮食习惯。",
        "我习惯早晨喝黑咖啡，已经坚持一年。另外，我坚持摄影五年，摄影是我的主要爱好。",
        "请记住我现在每天喝的饮料是黑咖啡，以及摄影这个长期爱好。",
    ])
    if variant == "paraphrase":
        return [initial,
            ("preference_first_correction", "乌龙茶", ["咖啡已停用，目前每天只喝乌龙茶。", "这会长期保持，不是临时换口味。", "已经戒咖啡，改喝乌龙茶，请更新当前偏好。"]),
            ("preference_second_correction", "白开水", ["乌龙茶也已停用，现在每天只喝白开水。", "已经戒掉茶和咖啡，这是新的长期习惯。", "请更新当前偏好，之前的饮料变化留作经历。"])]
    return [initial,
        ("preference_first_correction", "乌龙茶", ["从今天开始我不再喝黑咖啡，改为每天只喝乌龙茶。", "这是决定长期保持的新习惯。", "请更新当前偏好为乌龙茶，黑咖啡已经是过去的习惯。"]),
        ("preference_second_correction", "白开水", ["现在我也不喝乌龙茶了，改为每天只喝白开水。", "黑咖啡和乌龙茶都是过去的习惯，现在只喝白开水。", "这不是临时选择，请更新当前画像，保留之前的变化经历。"])]


def expected_cases(only):
    if only == "selection":
        return [case["name"] for case in selection_cases()]
    if only == "qualifiers":
        return [case["name"] for case in qualifier_cases()]
    names = ["preference_initial", "preference_first_correction", "preference_second_correction"]
    if only == "recall":
        names = names[:1]
    if only in {"all", "quality"}:
        names = ["low_value_chatter", "group_speaker_attribution", *names]
    if only not in QUALITY_MODES:
        names.append("real_vectors_from_generated_nodes")
    return names


def negative_mention(word):
    value = r"(?:黑)?咖啡" if word == "咖啡" else re.escape(word)
    return re.compile(r"(?:不再|从不|不|没有|未|停止|停用|戒掉|戒除|拒绝)(?:喜欢|习惯|饮用|喝|再|每天|只|常|了|的)*" + value)


def old_preference_status(text, word):
    """Reject leftover or historical mentions instead of guessing their meaning."""
    if word not in text:
        return "old_absent"
    for clause in re.split(r"[，。；;!?！？\n,]", text):
        if word in clause and (re.search(r"过去|以前|原来|曾", clause) or word in negative_mention(word).sub("", clause)):
            return "old_positive_or_ambiguous"
    return "old_only_explicit_current_negative"


def current_preference(text, expected):
    """Conservative fixture-specific lexical rule, not a semantic classifier."""
    if negative_mention(expected).search(text):
        return False
    return any(expected in clause and not re.search(r"过去|以前|原来|曾", clause)
               for clause in re.split(r"[，。；;!?！？\n,]", text))


def no_positive_hobby(text, hobby):
    """Permit explicit negatives; reject ambiguous leftover hobby mentions."""
    cleaned = re.sub(r"(?:不|并不|从不)(?:喜欢|爱好|擅长|会|弹|练习|进行)?" + re.escape(hobby), "", text)
    cleaned = re.sub(r"对" + re.escape(hobby) + r"(?:不感兴趣|没有兴趣)", "", cleaned)
    return hobby not in cleaned


def no_asserted_cat(texts):
    """Check every cat-bearing clause, including generated node narratives."""
    for text in texts:
        for clause in re.split(r"[，。；;!?！？\n,]", text):
            if "猫" in clause and not re.search(r"(?:没有猫|(?:未|尚未|不)(?:养|有)猫|(?:假设|如果|也许|可能|尚未决定|未决定).{0,16}猫)", clause):
                return False
    return True


async def run_checks(secrets, directory, *, proxy, http_budget=8, only="all", evidence=None, variant="standard"):
    from astrbot import __version__ as host_version
    from astrbot.core.provider.sources.openai_embedding_source import (
        OpenAIEmbeddingProvider,
    )
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
    from astrbot.core.star.context import Context

    from adapters.runtime import MemoryRuntime
    from core.config import NeoMemoryConfig
    from core.extraction_apply import digest
    from core.identity import EventIdentity
    from core.maintenance import MaintenanceEngine
    from core.memory_spaces import group_space, private_space
    from core.prompts import PROMPT_VERSION
    from core.source_ingest import CapturedSource
    from storage.memory_store import MemoryStore
    from storage.source_store import SourceStore

    llm_config, embed_config = secrets["ecoapi"], secrets["Cloudflare Embedding"]
    budget = HttpBudget(http_budget, llm_limit={"all": 6, "preference": 3, "recall": 1, "quality": 5, "preference-quality": 3, "qualifiers": 2, "selection": 3}[only],
                        embedding_limit=0 if only in QUALITY_MODES else (2 if only == "all" else 1))
    llm = ProviderOpenAIOfficial({"id": "isolated-llm", "type": "openai_chat_completion",
        "model": llm_config["model"], "api_base": llm_config["base_url"],
        "key": [llm_config["api_token"]], "timeout": 60}, {})
    embedding = OpenAIEmbeddingProvider({"id": "isolated-embedding", "type": "openai_embedding",
        "embedding_model": embed_config["model"],
        "embedding_api_base": embed_config["api_address"].rstrip("/") + "/accounts/" + embed_config["account_id"] + "/ai/v1",
        "embedding_api_key": embed_config["api_token"], "embedding_dimensions": 0,
        "embedding_dimensions_mode": "never", "timeout": 20, "proxy": proxy}, {})
    for kind, provider in (("llm", llm), ("embedding", embedding)):
        provider.client.max_retries = 0
        provider.client._client.event_hooks["request"].append(budget.hook(kind))
    providers = {"isolated-llm": llm, "isolated-embedding": embedding}

    async def get_provider_by_id(provider_id):
        return providers.get(provider_id)

    host = object.__new__(Context)
    host.provider_manager = SimpleNamespace(inst_map=providers, provider_insts=[llm],
        embedding_provider_insts=[embedding], get_provider_by_id=get_provider_by_id)
    config = NeoMemoryConfig(llm_provider_id="isolated-llm", embedding_provider_id="isolated-embedding",
        automatic_extraction_enabled=False, private_min_messages=3, private_topic_messages=3,
        private_max_messages=10, group_min_messages=3, group_topic_messages=3, group_max_messages=10,
        job_concurrency=1, job_poll_seconds=.05, job_max_attempts=1)
    sources = SourceStore(directory / "source.db")
    memory = MemoryStore(directory / "memory.db", source_exists=sources.has_source)
    plugin = SimpleNamespace(context=host, config=config, data_dir=directory, source_store=sources,
        memory_store=memory, maintenance=None, ready=True)
    runtime = MemoryRuntime(plugin)
    recorder = CandidateRecorder(runtime.adapter.generate) if evidence is not None else None
    if recorder is not None:
        runtime.adapter.generate = recorder

    async def persona_loader(_persona_id):
        return "合成测试人格：温和而克制的陪伴者，重视稳定偏好、学习进展与明确变化，不保留普通寒暄和假设为事实。"

    engine = MaintenanceEngine(sources, memory, runtime, config, persona_loader)
    if recorder is not None:
        recorder.observe_preparation(engine.extraction, engine.jobs)
    plugin.maintenance = engine
    report = {"tested_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
        "host_version": host_version, "llm_model": llm_config["model"], "embedding_model": embed_config["model"],
        "synthetic_only": True, "http_budget": http_budget, "provider_limits": budget.limits,
        "plugin_attempts": 1, "sdk_retries": 0, "extraction_timeout_seconds": config.extraction_timeout_seconds,
        "lowered_message_thresholds_for_replay": True, "only": only, "checks": [],
        "prompt_version": PROMPT_VERSION, "variant": variant, "evidence_enabled": evidence is not None,
        "expected_cases": expected_cases(only)}
    report["semantic_checks"] = "fixture_specific_lexical_rules_not_general_accuracy"
    sequence = 0
    users = {}
    case_sources, evidence_cases = {}, {}

    async def capture(messages, *, group=""):
        nonlocal sequence
        captured = []
        for user, text in messages:
            sequence += 1
            message_type = "GroupMessage" if group else "FriendMessage"
            session = group or user
            identity = EventIdentity("isolated-platform", "isolated", message_type,
                f"isolated:{message_type}:{session}", session, group, user, user, "synthetic-agent", f"m-{sequence}")
            record = await sources.capture(CapturedSource(identity=identity, actor_kind="user",
                content_text=text, observed_at=time.time() - 500 + sequence, actor_account_id=user,
                persona_id="synthetic_persona", metadata={"host_conversation_id": "synthetic:" + session}))
            users[user] = record.logical_user_id
            captured.append({"source_id": record.source_id, "sequence_no": record.sequence_no,
                "logical_user_id": record.logical_user_id, "actor_kind": "user", "content_text": text})
        spec = group_space("synthetic_persona", "isolated-platform", group) if group else private_space("synthetic_persona", users[messages[0][0]])
        space = await memory.get_or_create_space(spec)
        case_sources[space["space_id"]] = captured
        return space

    async def snapshot(space):
        space_id = space["space_id"]
        return evidence_snapshot(await memory.list_profiles(space_id=space_id),
            await memory.list_nodes(space_id=space_id, statuses=(), limit=500),
            [await memory.get_relationship(space_id=space_id, logical_user_id=user) for user in users.values()])

    def persist(check):
        if evidence is not None:
            data = evidence_cases.get(check["name"], {})
            data.update(name=check["name"], structure_ok=check["structure_ok"], check=dict(check),
                prompt_version=PROMPT_VERSION, synthetic_only=True)
            check["evidence_sha256"] = evidence.write_case(check["name"], data)

    def skip(name, reason):
        check = skipped(name, reason)
        report["checks"].append(check)
        persist(check)

    async def extract(name, space, expectations=None, review_requirements=None):
        capture_start = len(recorder.records) if recorder is not None else 0
        queued, job = None, None
        if evidence is not None:
            evidence_cases[name] = {"sources": case_sources[space["space_id"]], "before": await snapshot(space),
                "expectations": expectations or {}, "participants": dict(users)}
            if review_requirements:
                evidence_cases[name]["review_requirements"] = review_requirements
        started, before = time.perf_counter(), sum(budget.counts.values())
        if before >= budget.total or budget.counts["llm"] >= budget.limits["llm"]:
            check = skipped(name, "http_budget_exhausted")
        else:
            try:
                queued = await engine.enqueue("extract", space["space_id"], name)
                job, _ = await wait_terminal(engine, kind="extract", job_id=queued["job_id"], timeout=80)
                check = {"name": name, "structure_ok": job["state"] == "succeeded", "job": safe_job(job)}
            except Exception as exc:
                check = {"name": name, "structure_ok": False, **failure(exc)}
        check.update(seconds=round(time.perf_counter() - started, 4), http_requests=sum(budget.counts.values()) - before)
        check["review_required"] = evidence is not None
        if evidence is not None:
            evidence_cases[name]["after"] = await snapshot(space)
        generation = await memory.get_active_generation(space["space_id"])
        source_ids = [source["source_id"] for source in case_sources[space["space_id"]]]
        consumed = await engine.pipeline.consumed(generation["generation_id"], source_ids)
        record_consumption(check, source_ids, consumed)
        if evidence is not None:
            receipt = None
            if queued is not None:
                payload = queued["payload"]
                batch_id = "batch_" + digest([space["space_id"], payload["expected_generation_id"], tuple(payload["source_ids"])])
                receipt = await memory.get_extraction_batch(batch_id)
            data = evidence_cases[name]
            data["model_candidates"] = recorder.records[capture_start:]
            data["commit_receipt"] = ({key: receipt[key] for key in
                ("batch_id", "input_digest", "output_digest", "prompt_version", "schema_version")}
                if receipt is not None else None)
            check["candidate_diagnostics"] = diagnose_profiles(data["model_candidates"], data["before"], data["after"], receipt,
                terminal=job is not None and job["state"] not in {"queued", "running", "retry_wait"},
                rejection_reason=job.get("error_code") if job is not None else None)
            if recorder.overflow_count:
                check["candidate_diagnostics"].update(status="evidence_incomplete", consistency_ok=None,
                    overflow_count=recorder.overflow_count)
            if check["structure_ok"] and check["candidate_diagnostics"]["consistency_ok"] is not True:
                check.update(structure_ok=False, reason="candidate_application_unverified")
        report["checks"].append(check)
        return check

    try:
        await sources.open()
        await memory.open()
        await runtime.start()
        await engine.start()
        if only == "selection":
            completed = {}
            for case in selection_cases():
                if case.get("depends_on") and not completed.get(case["depends_on"]):
                    skip(case["name"], "prerequisite_structure_failed")
                    continue
                private = await capture([(case["user"], text) for text in case["messages"]])
                check = await extract(case["name"], private, review_requirements=case["review_requirements"])
                profiles = await memory.list_profiles(space_id=private["space_id"])
                finish(check, {"profile_presence": bool(profiles) == case["profile_expected"]})
                persist(check)
                completed[case["name"]] = continue_preference_chain(check)
            return report
        if only == "qualifiers":
            prior_ok = True
            for case in qualifier_cases():
                if not prior_ok:
                    skip(case["name"], "prerequisite_structure_failed")
                    continue
                private = await capture([("qualifier-user", text) for text in case["messages"]])
                check = await extract(case["name"], private, review_requirements=case["review_requirements"])
                # No keyword predicate can certify conditional scope. These
                # checks confirm artifacts exist; question review is required.
                profiles = await memory.list_profiles(space_id=private["space_id"])
                nodes = await memory.list_nodes(space_id=private["space_id"])
                finish(check, {"profile_exists": bool(profiles), "nodes_exist": bool(nodes)})
                persist(check)
                prior_ok = continue_preference_chain(check)
            return report
        group = await memory.get_or_create_space(group_space("synthetic_persona", "isolated-platform", "synthetic-group"))
        if only in {"all", "quality"}:
            chatter_space = await capture([("chatter", text) for text in ("早上好。", "哈哈。", "好的。")])
            check = await extract("low_value_chatter", chatter_space,
                review_requirements=regression_requirements("low_value_chatter"))
            finish(check, {"all_sources_consumed": check["all_sources_consumed"],
                "no_long_term_nodes": not await memory.list_nodes(space_id=chatter_space["space_id"], statuses=()),
                "no_profile": not await memory.list_profiles(space_id=chatter_space["space_id"]),
                "no_relationship": not await memory.get_relationship(space_id=chatter_space["space_id"], logical_user_id=users["chatter"])})
            persist(check)

            group = await capture([
                ("alice", "我是小岚。我已经坚持摄影五年了，摄影一直是我的主要爱好。"),
                ("bob", "我是小舟。我从小就喜欢钢琴，每周都练习钢琴，这一直是我的主要爱好。"),
                ("alice", "我每周六都会外出拍风景，已经保持五年，我不弹钢琴。"),
                ("bob", "我主要在家练琴，我不喜欢摄影。"),
                ("charlie", "假设我以后养猫，也许会天天拍照。但我现在没有猫，也还没决定养。"),
                ("charlie", "刚才只是举例，大家不用把假设当作已经发生的事。"),
            ], group="synthetic-group")
            check = await extract("group_speaker_attribution", group,
                review_requirements=regression_requirements("group_speaker_attribution"))
            profiles = await memory.list_profiles(space_id=group["space_id"])
            a, b, c = (profile_text(profiles, users[name]) for name in ("alice", "bob", "charlie"))
            group_nodes = await memory.list_nodes(space_id=group["space_id"])
            relationships = [await memory.get_relationship(space_id=group["space_id"], logical_user_id=users[n]) for n in ("alice", "bob", "charlie")]
            finish(check, {"alice_photography_profile": "摄影" in a,
                "bob_piano_profile": "钢琴" in b,
                "no_cross_member_positive_hobby": no_positive_hobby(a, "钢琴") and no_positive_hobby(b, "摄影"),
                "profiles_use_known_members": all(p["logical_user_id"] in {users[n] for n in ("alice", "bob", "charlie")} for p in profiles),
                "lightweight_profiles": all(len(p["description"]) <= 240 and len(p["tags"]) <= 8 for p in profiles),
                "hypothetical_owner_not_invented": no_asserted_cat([c, *(n["content_text"] for n in group_nodes)]),
                "no_group_relationship": not any(relationships)})
            persist(check)

        cases = preference_cases(variant)
        if only == "recall":
            cases = cases[:1]
        prior_ok, private, initial_ids = True, None, set()
        for index, (name, expected, texts) in enumerate(cases):
            if not prior_ok:
                skip(name, "prerequisite_structure_failed")
                continue
            private = await capture([("preference-user", text) for text in texts])
            check = await extract(name, private, {"current_drink": expected,
                "retained_hobby": "摄影", "past_drinks": ["咖啡", "乌龙茶"][:index]},
                review_requirements=preference_requirements(expected, index))
            profiles = await memory.list_profiles(space_id=private["space_id"])
            text = profile_text(profiles, users["preference-user"])
            nodes = await memory.list_nodes(space_id=private["space_id"])
            assertions = {"current_profile_lexical_rule": current_preference(text, expected), "nonempty_long_term": bool(nodes),
                "unrelated_hobby_preserved_lexical_rule": "摄影" in text}
            if index == 0:
                initial_ids = {n["node_id"] for n in nodes if "咖啡" in n["content_text"]}
            else:
                old_statuses = {word: old_preference_status(text, word) for word in (["咖啡"] if index == 1 else ["咖啡", "乌龙茶"])}
                check["old_preference_diagnostics"] = old_statuses
                assertions["no_positive_or_historical_old_profile"] = all(value != "old_positive_or_ambiguous" for value in old_statuses.values())
                assertions["existing_node_retains_history"] = any(n["node_id"] in initial_ids and n["content_version"] > 1
                    and all(word in n["content_text"] for word in (["咖啡", "乌龙茶"] if index == 1 else ["咖啡", "乌龙茶", "白开水"])) for n in nodes)
            finish(check, assertions)
            persist(check)
            prior_ok = continue_preference_chain(check)

        if only in QUALITY_MODES:
            return report

        if not prior_ok or private is None:
            skip("real_vectors_from_generated_nodes", "prerequisite_structure_failed")
        elif budget.total - sum(budget.counts.values()) < 1:
            skip("real_vectors_from_generated_nodes", "http_budget_exhausted")
        else:
            started, before = time.perf_counter(), sum(budget.counts.values())
            check = {"name": "real_vectors_from_generated_nodes", "structure_ok": False}
            try:
                query = "我现在每天只喝什么饮料？"
                nodes = await memory.list_nodes(space_id=private["space_id"])
                texts = [n["content_text"] for n in nodes] + [query]
                if len(set(texts)) > config.embedding_batch_size or sum(len(text.encode()) for text in set(texts)) > 16000:
                    raise ValueError("synthetic_embedding_batch_budget_exceeded")
                # The index reuses exact-text message vectors under versioned
                # node keys; this is one real batch, not a network retry.
                await runtime.recall.embeddings.embed(texts, namespace=private["space_id"], timeout_seconds=20)
                batch_completed = sum(budget.counts.values())
                indexed = await runtime.recall.index_space(private["space_id"], timeout_seconds=config.index_timeout_seconds)
                index_requests = sum(budget.counts.values()) - batch_completed
                warmed = sum(budget.counts.values())
                warm_started = time.perf_counter()
                recalled = await runtime.recall.recall(private["space_id"], query)
                warm_seconds = time.perf_counter() - warm_started
                other = await runtime.recall.recall(group["space_id"], query)
                check["structure_ok"] = True
                expected_drink = cases[-1][1]
                finish(check, {"actual_generated_node_recalled": bool(recalled.hits) and any(expected_drink in n["content_text"] for n in recalled.hits),
                    "recalled_ids_from_generated_space": all(n["node_id"] in {item["node_id"] for item in nodes} for n in recalled.hits),
                    "no_shared_vector_leak_to_other_space": not other.hits,
                    "index_reuses_message_vectors": index_requests == 0,
                    "warm_recall_no_http": sum(budget.counts.values()) == warmed,
                    "within_default_deadline": not recalled.diagnostics.get("deadline_exceeded"),
                    "within_default_budget": len(recalled.text.encode()) <= config.recall_budget_tokens})
                check.update(indexed_nodes=indexed["indexed"], recalled_nodes=len(recalled.hits), warm_recall_seconds=round(warm_seconds, 4),
                    other_space_cache_cold=True, other_space_empty=only != "all", batched_node_and_query_vectors=True,
                    node_key_fill_http_requests=index_requests)
            except Exception as exc:
                check.update(failure(exc))
                finish(check, {})
            check.update(seconds=round(time.perf_counter() - started, 4), http_requests=sum(budget.counts.values()) - before)
            report["checks"].append(check)
    except Exception as exc:
        report["run_error"] = failure(exc)
    finally:
        report["http_requests"] = dict(budget.counts)
        report["http_requests_total"] = sum(budget.counts.values())
        for close in (engine.stop, runtime.close, memory.close, sources.close, llm.terminate, embedding.terminate):
            try:
                await close()
            except Exception as exc:
                report.setdefault("cleanup_errors", []).append(failure(exc))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    parser.add_argument("--proxy", default="http://192.168.5.100:7890")
    parser.add_argument("--http-budget", type=int, choices=range(1, 9), default=8)
    parser.add_argument("--only", choices=("all", "preference", "recall", "quality", "preference-quality", "qualifiers", "selection"), default="all")
    parser.add_argument("--evidence-dir", type=Path, help="New local directory for synthetic evidence; never overwritten")
    parser.add_argument("--variant", choices=("standard", "paraphrase"), default="standard")
    args = parser.parse_args()
    if args.evidence_dir is not None and args.only not in QUALITY_MODES:
        parser.error("--evidence-dir requires a quality mode")
    if args.only in QUALITY_MODES and args.evidence_dir is None:
        parser.error("quality modes require --evidence-dir for offline review")
    logging.disable(logging.CRITICAL)
    try:
        evidence = EvidenceWriter(args.evidence_dir) if args.evidence_dir is not None else None
        with tempfile.TemporaryDirectory(prefix="neomemory-stability-api-") as temporary:
            directory = Path(temporary)
            (directory / "data/config").mkdir(parents=True)
            os.environ["ASTRBOT_ROOT"] = str(directory)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                from loguru import logger
                logger.remove()
                report = asyncio.run(run_checks(credentials(args.credentials), directory, proxy=args.proxy,
                    http_budget=args.http_budget, only=args.only, evidence=evidence, variant=args.variant))
            if evidence is not None:
                evidence.write_report(report)
    except Exception as exc:
        report = {"setup_or_run": failure(exc)}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    checks = report.get("checks", [])
    expected = len(expected_cases(args.only))
    if args.evidence_dir is not None and len(checks) == expected and all(check.get("structure_ok") for check in checks) and not report.get("run_error") and not report.get("cleanup_errors"):
        return 2  # Complete evidence awaits independent semantic review.
    return 0 if len(checks) == expected and all(check["ok"] for check in checks) and not report.get("run_error") and not report.get("cleanup_errors") else 1


if __name__ == "__main__":
    raise SystemExit(main())
