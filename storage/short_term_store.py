"""Fenced, atomic rolling summary updates with durable commit receipts."""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .job_store import assert_job_lease


class ShortTermStore:
    def __init__(self, memory):
        self.memory = memory
        self.database = memory.database

    async def receipt(self, job_id: str) -> dict[str, Any] | None:
        row = await self.database.fetchone(
            "SELECT result_json FROM short_term_receipts WHERE job_id=?", (job_id,),
        )
        return json.loads(row["result_json"]) if row else None

    async def get(self, space_id: str, conversation_key: str, *, generation_id: str | None = None):
        return await self.memory.get_short_term(
            space_id=space_id, conversation_id=conversation_key, generation_id=generation_id,
        )

    async def apply(self, job, *, input_digest: str, summary_text: str,
                    records: list[dict[str, Any]], evidence_source_ids: list[str]) -> dict[str, Any]:
        payload = job["payload"]
        now = time.time()
        async with self.database.transaction() as connection:
            current_job = await assert_job_lease(connection, job["job_id"], job["lease_token"],
                                                  scope_key=job["scope_key"])
            if current_job["kind"] != "short_term" or current_job["payload"] != payload:
                raise ValueError("summary task mismatch")
            cursor = await connection.execute(
                "SELECT * FROM short_term_receipts WHERE job_id=?", (job["job_id"],),
            )
            receipt = await cursor.fetchone()
            if receipt:
                if receipt["input_digest"] != input_digest:
                    raise ValueError("summary receipt mismatch")
                return json.loads(receipt["result_json"])
            cursor = await connection.execute(
                "SELECT generation_id FROM memory_generations WHERE space_id=? AND status='active'",
                (job["scope_key"],),
            )
            generation = await cursor.fetchone()
            if not generation or generation["generation_id"] != payload["generation_id"]:
                raise ValueError("summary generation changed")
            cursor = await connection.execute(
                "SELECT * FROM short_term_states WHERE generation_id=? AND space_id=? AND conversation_id=?",
                (payload["generation_id"], job["scope_key"], payload["conversation_key"]),
            )
            prior = await cursor.fetchone()
            if (int(prior["content_version"]) if prior else 0) != payload["previous_version"]:
                raise ValueError("summary version changed")
            if prior and prior["status"] != "active":
                raise ValueError("summary disabled")
            old_coverage = json.loads(prior["coverage_json"]) if prior else {}
            if not records or [row["source_id"] for row in records] != payload["source_ids"]:
                raise ValueError("summary sources changed")
            if records[0]["sequence_no"] <= old_coverage.get("to_sequence", 0):
                raise ValueError("summary source range overlaps")
            coverage = {
                "strategy": "rolling_prediction.v1", "stream_id": payload["stream_id"],
                "host_conversation_id": payload["conversation_id"],
                "from_sequence": old_coverage.get("from_sequence", records[0]["sequence_no"]),
                "to_sequence": records[-1]["sequence_no"],
                "from_observed_at": old_coverage.get("from_observed_at", records[0]["observed_at"]),
                "to_observed_at": records[-1]["observed_at"],
                "source_count": old_coverage.get("source_count", 0) + len(records),
                "last_evidence_source_ids": evidence_source_ids,
                "keep_messages": payload["keep_messages"],
            }
            state_id = prior["state_id"] if prior else f"short_{uuid.uuid4().hex}"
            version = payload["previous_version"] + 1
            if prior:
                await connection.execute(
                    "UPDATE short_term_states SET summary_text=?,covered_to_source_id=?,coverage_json=?,"
                    "content_version=?,updated_at=? WHERE state_id=?",
                    (summary_text, records[-1]["source_id"], json.dumps(coverage, ensure_ascii=False),
                     version, now, state_id),
                )
            else:
                await connection.execute(
                    "INSERT INTO short_term_states(state_id,generation_id,space_id,conversation_id,summary_text,"
                    "covered_from_source_id,covered_to_source_id,coverage_json,created_at,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (state_id, payload["generation_id"], job["scope_key"], payload["conversation_key"],
                     summary_text, records[0]["source_id"], records[-1]["source_id"],
                     json.dumps(coverage, ensure_ascii=False), now, now),
                )
            await connection.executemany(
                "INSERT INTO short_term_sources(state_id,source_id,sequence_no) VALUES (?,?,?)",
                [(state_id, row["source_id"], row["sequence_no"]) for row in records],
            )
            result = {"state_id": state_id, "content_version": version,
                      "generation_id": payload["generation_id"], "covered_count": coverage["source_count"]}
            await connection.execute(
                "INSERT INTO short_term_receipts(job_id,state_id,input_digest,result_json,committed_at) VALUES (?,?,?,?,?)",
                (job["job_id"], state_id, input_digest, json.dumps(result), now),
            )
            await assert_job_lease(connection, job["job_id"], job["lease_token"], scope_key=job["scope_key"])
            return result
