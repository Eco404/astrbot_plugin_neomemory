"""Confirmed account association changes with durable cross-database recovery."""
from __future__ import annotations

import hashlib
import json
import time
import uuid

try:
    from ..storage.social_accounts import account_namespace, account_routes, group_accounts
    from ..storage.job_store import JobConflict, assert_job_lease
except ImportError:
    from storage.social_accounts import account_namespace, account_routes, group_accounts
    from storage.job_store import JobConflict, assert_job_lease
from .jobs import ReviewRequired


class IdentityConflict(ValueError):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()


def _identity_digest(identity):
    def ownership(user):
        if user is None:
            return None
        fields = ("account_namespace", "account_id", "logical_user_id", "binding_status")
        accounts = [{key: account[key] for key in fields} for account in user["accounts"]]
        accounts.sort(key=lambda item: (item["account_namespace"], item["account_id"]))
        return {"logical_user_id": user["logical_user_id"], "status": user["status"], "accounts": accounts}

    account = identity["account"]
    return _digest({"action": identity["action"], "target_user": ownership(identity["target_user"]),
                    "previous_user": ownership(identity["previous_user"]),
                    "account": {"account_namespace": account["account_namespace"], "account_id": account["account_id"]},
                    "delete_previous_user": identity["delete_previous_user"]})


async def _one(connection, sql, parameters=()):
    cursor = await connection.execute(sql, parameters)
    row = await cursor.fetchone()
    return dict(row) if row is not None else None


class IdentityManagement:
    def __init__(self, sources, memory, jobs, runtime):
        self.sources, self.memory, self.jobs, self.runtime = sources, memory, jobs, runtime

    async def _user(self, connection, user_id):
        row = await _one(connection,
            "SELECT logical_user_id,display_name,status,display_name_source FROM logical_users "
            "WHERE logical_user_id=? AND status<>'deleted'", (user_id,))
        if row is None:
            raise KeyError(user_id)
        cursor = await connection.execute(
            "SELECT account_namespace,platform_instance_id,account_id,logical_user_id,binding_status,source "
            "FROM account_routes WHERE logical_user_id=? ORDER BY platform_instance_id,account_id", (user_id,))
        row["accounts"] = group_accounts([dict(account) for account in await cursor.fetchall()])
        return row

    async def get_space(self, space_id):
        space = await self.memory.get_space(space_id)
        if not space or space["space_id"] != space_id:
            raise KeyError(space_id)
        return space

    async def _identity(self, connection, *, action, logical_user_id, platform_instance_id, account_id, space=None):
        if space is not None:
            members, params = self.sources.space_user_query(space)
            allowed = await _one(connection, f"SELECT 1 WHERE ? IN ({members})", (logical_user_id, *params))
            if not allowed:
                raise IdentityConflict("identity_outside_space", "该逻辑用户不属于所选记忆空间。")
        target = await self._user(connection, logical_user_id)
        routes = await account_routes(connection, platform_instance_id, account_id)
        owners = {route["logical_user_id"] for route in routes}
        route = next((item for item in routes if item["platform_instance_id"] == platform_instance_id), None)
        if len(owners) > 1 and not route:
            raise IdentityConflict("ambiguous_account", "该账号存在多个归属，请选择一个已有入口预览转移。")
        previous_id = route["logical_user_id"] if route else next(iter(owners), None)
        previous = await self._user(connection, previous_id) if previous_id else None
        instances = {item["platform_instance_id"] for item in routes if item["logical_user_id"] == previous_id}
        instances.add(platform_instance_id)
        namespace = routes[0]["account_namespace"] if routes else await account_namespace(connection, platform_instance_id)
        if action == "bind":
            if previous and previous["logical_user_id"] == logical_user_id:
                raise IdentityConflict("account_already_bound", "该账号已属于此逻辑用户，无需重复添加。")
        elif action == "unbind":
            if previous_id != logical_user_id:
                raise IdentityConflict("account_binding_changed", "该账号已不属于当前逻辑用户，请刷新列表。")
            if len(target["accounts"]) < 2:
                raise IdentityConflict("last_account", "逻辑用户必须至少保留一个账号，不能移除唯一账号。")
        else:
            raise ValueError("unsupported identity action")
        return {"action": action, "target_user": target, "previous_user": previous,
                "account": {"platform_instance_id": platform_instance_id, "platform_instance_ids": sorted(instances),
                            "account_namespace": namespace, "account_id": account_id},
                "delete_previous_user": action == "bind" and previous is not None and len(previous["accounts"]) == 1}

    async def _counts(self, connection, user_id):
        spaces = "SELECT space_id FROM memory_spaces WHERE space_type='private' AND logical_user_id=?"
        counts = {}
        queries = {
            "private_spaces": f"SELECT COUNT(*) AS n FROM memory_spaces WHERE space_id IN ({spaces})",
            "nodes": f"SELECT COUNT(*) AS n FROM memory_nodes WHERE space_id IN ({spaces})",
            "node_versions": f"SELECT COUNT(*) AS n FROM memory_node_versions WHERE node_id IN (SELECT node_id FROM memory_nodes WHERE space_id IN ({spaces}))",
            "profiles": f"SELECT COUNT(*) AS n FROM user_profiles WHERE space_id IN ({spaces})",
            "profile_versions": f"SELECT COUNT(*) AS n FROM profile_versions WHERE profile_id IN (SELECT profile_id FROM user_profiles WHERE space_id IN ({spaces}))",
            "relationships": f"SELECT COUNT(*) AS n FROM relationship_states WHERE space_id IN ({spaces})",
            "relationship_versions": f"SELECT COUNT(*) AS n FROM relationship_versions WHERE relationship_id IN (SELECT relationship_id FROM relationship_states WHERE space_id IN ({spaces}))",
            "short_term": f"SELECT COUNT(*) AS n FROM short_term_states WHERE space_id IN ({spaces})",
            "notes": f"SELECT COUNT(*) AS n FROM notes WHERE space_id IN ({spaces})",
            "tasks": f"SELECT COUNT(*) AS n FROM memory_jobs WHERE scope_key IN ({spaces})",
        }
        for key, query in queries.items():
            counts[key] = (await _one(connection, query, (user_id,)))["n"] if user_id else 0
        return counts

    async def _private_spaces(self, connection, user_id):
        cursor = await connection.execute(
            "SELECT space_id,persona_id FROM memory_spaces WHERE space_type='private' "
            "AND logical_user_id=? ORDER BY persona_id,space_id", (user_id,))
        return [dict(row) for row in await cursor.fetchall()]

    async def account_conflicts(self, users):
        if not users:
            return []
        ids = [user["logical_user_id"] for user in users]
        marks = ",".join("?" for _ in ids)
        rows = await self.sources.database.fetchall(
            "WITH relevant AS (SELECT DISTINCT account_namespace,account_id FROM account_routes "
            f"WHERE logical_user_id IN ({marks})), conflicts AS ("
            "SELECT r.account_namespace,r.account_id FROM account_routes r JOIN relevant USING(account_namespace,account_id) "
            "GROUP BY r.account_namespace,r.account_id HAVING COUNT(DISTINCT r.logical_user_id)>1) "
            "SELECT r.*,u.display_name FROM conflicts JOIN account_routes r USING(account_namespace,account_id) "
            "JOIN logical_users u ON u.logical_user_id=r.logical_user_id "
            "ORDER BY r.account_namespace,r.account_id,r.logical_user_id,r.platform_instance_id", ids)
        if not rows:
            return []
        conflicts = {}
        impacts = {}
        async with self.memory.database.transaction(write=False) as memory:
            for owner in group_accounts([dict(row) for row in rows]):
                key = (owner["account_namespace"], owner["account_id"])
                entry = conflicts.setdefault(key, {"account_namespace": key[0], "account_id": key[1], "owners": []})
                user_id = owner["logical_user_id"]
                if user_id not in impacts:
                    impacts[user_id] = {"private_spaces": await self._private_spaces(memory, user_id),
                                        "counts": await self._counts(memory, user_id)}
                entry["owners"].append({
                    "logical_user_id": user_id, "display_name": owner["display_name"],
                    "platform_instance_ids": owner["platform_instance_ids"], **impacts[user_id]})
        return list(conflicts.values())

    async def preview(self, *, space_id=None, **request):
        space = await self.get_space(space_id) if space_id else None
        async with self.sources.database.transaction(write=False) as connection:
            identity = await self._identity(connection, space=space, **request)
        old_id = identity["previous_user"]["logical_user_id"] if identity["delete_previous_user"] else None
        async with self.memory.database.transaction(write=False) as memory:
            counts = await self._counts(memory, old_id)
            previous_spaces = []
            if identity["action"] == "bind" and identity["previous_user"]:
                previous_spaces = await self._private_spaces(memory, identity["previous_user"]["logical_user_id"])
        preview = {**identity, "counts": counts, "raw_records_preserved": True,
                   "shared_group_memories_preserved": True, "previous_private_spaces": previous_spaces}
        if space_id:
            preview["space_id"] = space_id
        return {**preview, "confirmation_token": _digest(preview)}

    async def submit(self, *, confirmation_token, idempotency_key, **request):
        existing = await self.jobs.database.fetchone(
            "SELECT job_id FROM memory_jobs WHERE idempotency_key=?", (idempotency_key,))
        if existing:
            job = await self.jobs.get(existing["job_id"])
            if job["kind"] != "identity" or job["payload"].get("request") != request or job["payload"].get("confirmation_token") != confirmation_token:
                raise JobConflict("idempotency key already describes another operation")
            return job
        preview = await self.preview(**request)
        if preview["confirmation_token"] != confirmation_token:
            raise IdentityConflict("identity_confirmation_changed", "账号归属或待删除内容已变化，请重新预览并确认。")
        payload = {"request": request, "confirmation_token": confirmation_token,
                   "identity_digest": _identity_digest(preview), "preview": preview,
                   "new_logical_user_id": "user_" + uuid.uuid4().hex if request["action"] == "unbind" else None}
        return await self.jobs.submit("identity", "*", payload, idempotency_key)

    async def _purge_memory(self, job, old_user):
        async with self.memory.database.transaction() as connection:
            current = await assert_job_lease(connection, job["job_id"], job["lease_token"], scope_key="*")
            if current["checkpoint"].get("identity_purge_complete"):
                return current["checkpoint"]["counts"]
            counts = await self._counts(connection, old_user)
            if counts != job["payload"]["preview"]["counts"]:
                raise ReviewRequired("identity_confirmation_changed")
            if old_user:
                cursor = await connection.execute(
                    "SELECT space_id FROM memory_spaces WHERE space_type='private' AND logical_user_id=? ORDER BY persona_id,space_id", (old_user,))
                spaces = [row["space_id"] for row in await cursor.fetchall()]
                confirmed = job["payload"]["preview"].get("previous_private_spaces")
                if confirmed is not None and spaces != [row["space_id"] for row in confirmed]:
                    raise ReviewRequired("identity_confirmation_changed")
                for space_id in spaces:
                    cursor = await connection.execute("SELECT node_id FROM memory_nodes WHERE space_id=?", (space_id,))
                    node_ids = [row["node_id"] for row in await cursor.fetchall()]
                    if node_ids and self.runtime.cache is not None:
                        await self.runtime.cache.invalidate_nodes(space_id, node_ids)
                    cursor = await connection.execute("SELECT job_id,scope_key FROM memory_jobs WHERE scope_key=?", (space_id,))
                    for old_job in await cursor.fetchall():
                        await self.jobs._delete_stopped(connection, old_job)
                    await connection.execute("DELETE FROM memory_spaces WHERE space_id=?", (space_id,))
                await connection.execute(
                    "INSERT INTO pipeline_state(state_key,value_json,updated_at) VALUES (?,'true',?) "
                    "ON CONFLICT(state_key) DO UPDATE SET value_json='true',updated_at=excluded.updated_at",
                    ("retired_identity:" + old_user, time.time()))
            checkpoint = {"identity_purge_complete": True, "counts": counts}
            await connection.execute(
                "UPDATE memory_jobs SET checkpoint_json=?,step='identity_apply',progress_done=1,progress_total=2,updated_at=? WHERE job_id=?",
                (json.dumps(checkpoint), time.time(), job["job_id"]))
            return counts

    async def run(self, job):
        async with self.sources.database.transaction() as connection:
            receipt = await _one(connection, "SELECT result_json FROM identity_operation_receipts WHERE job_id=?", (job["job_id"],))
            if receipt:
                return json.loads(receipt["result_json"])
            payload = job["payload"]
            try:
                request = dict(payload["request"])
                space_id = request.pop("space_id", None)
                space = await self.get_space(space_id) if space_id else None
                identity = await self._identity(connection, space=space, **request)
                if _identity_digest(identity) != payload["identity_digest"]:
                    raise ReviewRequired("identity_confirmation_changed")
            except (IdentityConflict, KeyError) as exc:
                raise ReviewRequired("identity_confirmation_changed") from exc
            old_user = identity["previous_user"]["logical_user_id"] if identity["delete_previous_user"] else None
            counts = await self._purge_memory(job, old_user)
            request = payload["request"]
            target = request["logical_user_id"]
            now = time.time()
            if request["action"] == "unbind":
                target = payload["new_logical_user_id"]
                await connection.execute(
                    "INSERT INTO logical_users(logical_user_id,display_name,status,created_at,updated_at) VALUES (?,?,'active',?,?)",
                    (target, request["account_id"], now, now))
            account = identity["account"]
            await connection.executemany(
                "INSERT INTO account_bindings(platform_instance_id,account_id,logical_user_id,binding_status,source,created_at,updated_at) "
                "VALUES (?,?,?,'active','manual',?,?) ON CONFLICT(platform_instance_id,account_id) DO UPDATE SET "
                "logical_user_id=excluded.logical_user_id,binding_status='active',source='manual',updated_at=excluded.updated_at",
                [(instance, account["account_id"], target, now, now) for instance in account["platform_instance_ids"]])
            if old_user:
                await connection.execute("UPDATE logical_users SET status='deleted',updated_at=? WHERE logical_user_id=?", (now, old_user))
            result = {"action": request["action"], "logical_user_id": target, "account": identity["account"],
                      "deleted_logical_user_id": old_user, "removed": counts, "raw_records_preserved": True}
            await connection.execute("INSERT INTO identity_operation_receipts(job_id,result_json,committed_at) VALUES (?,?,?)",
                                     (job["job_id"], json.dumps(result, ensure_ascii=False), now))
        return result

    async def cleanup_receipts(self):
        """Remove recovery receipts only after their durable task has been deleted."""
        after = ""
        removed = 0
        while True:
            rows = await self.sources.database.fetchall(
                "SELECT job_id FROM identity_operation_receipts WHERE job_id>? ORDER BY job_id LIMIT 200", (after,))
            if not rows:
                return removed
            ids = [row["job_id"] for row in rows]
            placeholders = ",".join("?" for _ in ids)
            existing = await self.jobs.database.fetchall(
                f"SELECT job_id FROM memory_jobs WHERE job_id IN ({placeholders})", ids)
            retained = {row["job_id"] for row in existing}
            async with self.sources.database.transaction() as connection:
                for job_id in ids:
                    if job_id not in retained:
                        cursor = await connection.execute("DELETE FROM identity_operation_receipts WHERE job_id=?", (job_id,))
                        removed += cursor.rowcount
            after = ids[-1]
