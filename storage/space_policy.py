"""Personas authorize shared recall; each source space controls its visibility."""

PERSONA_DEFAULTS = {
    "shared_recall_enabled": False,
    "allow_group_read_private": False,
    "external_limit": 2,
    "external_budget_percent": 20,
}
SPACE_DEFAULTS = {
    "exclude_from_shared_recall": False,
    "model_mode": "inherit",
    "provider_id": "",
    "topic_follow_main": False,
    "topic_provider_id": "",
    "short_term_follow_main": False,
    "short_term_provider_id": "",
}

POLICY_SCHEMA = (
    """CREATE TABLE persona_policies (
        persona_id TEXT PRIMARY KEY,
        shared_recall_enabled INTEGER NOT NULL DEFAULT 0,
        allow_group_read_private INTEGER NOT NULL DEFAULT 0,
        external_limit INTEGER NOT NULL DEFAULT 2,
        external_budget_percent INTEGER NOT NULL DEFAULT 20)""",
    """CREATE TABLE space_policies (
        space_id TEXT PRIMARY KEY REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        exclude_from_shared_recall INTEGER NOT NULL DEFAULT 0,
        model_mode TEXT NOT NULL DEFAULT 'inherit',
        provider_id TEXT NOT NULL DEFAULT '',
        topic_provider_id TEXT NOT NULL DEFAULT '')""",
    "CREATE INDEX memory_spaces_by_persona ON memory_spaces(persona_id, space_id)",
)


async def _policy(connection, space_id):
    row = await (await connection.execute(
        "SELECT s.space_id,s.persona_id,p.shared_recall_enabled,p.allow_group_read_private,"
        "p.external_limit,p.external_budget_percent,r.exclude_from_shared_recall,r.model_mode,"
        "r.provider_id,r.topic_provider_id,r.short_term_provider_id,r.topic_follow_main,r.short_term_follow_main "
        "FROM memory_spaces s "
        "LEFT JOIN persona_policies p USING(persona_id) LEFT JOIN space_policies r USING(space_id) "
        "WHERE s.space_id=?", (space_id,))).fetchone()
    if row is None:
        raise KeyError("unknown space")
    result = dict(row)
    for key, default in (PERSONA_DEFAULTS | SPACE_DEFAULTS).items():
        if result[key] is None:
            result[key] = default
        elif type(default) is bool:
            result[key] = bool(result[key])
    return result


class SpacePolicyStoreMixin:
    async def get_space_policy(self, space_id):
        async with self.database.transaction(write=False) as connection:
            return await _policy(connection, space_id)

    async def set_space_policy(self, space_id, changes):
        defaults = PERSONA_DEFAULTS | SPACE_DEFAULTS
        for key, value in changes.items():
            if key not in defaults or type(value) is not type(defaults[key]):
                raise ValueError("invalid space policy field")
        changes = dict(changes)
        for lane in ("topic", "short_term"):
            follow, provider = f"{lane}_follow_main", f"{lane}_provider_id"
            if changes.get(follow):
                changes[provider] = ""
            elif changes.get(provider):
                changes[follow] = False
        async with self.database.transaction() as connection:
            result = await _policy(connection, space_id)
            result.update(changes)
            if result["model_mode"] not in {"inherit", "fixed", "chat"} or (
                result["model_mode"] == "fixed" and not result["provider_id"].strip()
            ):
                raise ValueError("invalid model selection")
            if not 0 <= result["external_limit"] <= 10 or not 0 <= result["external_budget_percent"] <= 100:
                raise ValueError("invalid shared recall budget")
            for table, primary, fields in (("persona_policies", "persona_id", PERSONA_DEFAULTS),
                                           ("space_policies", "space_id", SPACE_DEFAULTS)):
                keys = [key for key in fields if key in changes]
                if keys:
                    columns = [primary, *keys]
                    await connection.execute(
                        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)}) "
                        f"ON CONFLICT({primary}) DO UPDATE SET " + ",".join(f"{k}=excluded.{k}" for k in keys),
                        [result[k] for k in columns])
            return result

    async def list_shared_spaces(self, space_id):
        rows = await self.database.fetchall(
            "SELECT source.* FROM memory_spaces current JOIN persona_policies p USING(persona_id) "
            "JOIN memory_spaces source ON source.persona_id=current.persona_id AND source.space_id<>current.space_id "
            "LEFT JOIN space_policies r ON r.space_id=source.space_id "
            "WHERE current.space_id=? AND p.shared_recall_enabled=1 AND COALESCE(r.exclude_from_shared_recall,0)=0 "
            "AND (current.space_type<>'group' OR source.space_type<>'private' OR p.allow_group_read_private=1) "
            "ORDER BY source.space_id", (space_id,))
        return [dict(row) for row in rows]
