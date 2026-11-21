"""Resolve social accounts across adapter instances that expose a shared user ID."""

from __future__ import annotations

try:
    from ..core.identity import default_logical_user_id
except ImportError:
    from core.identity import default_logical_user_id


SHARED_NAMESPACES = {"aiocqhttp": "qq", "telegram": "telegram", "discord": "discord"}


class AmbiguousAccountOwner(Exception):
    """A new entry point has several existing owners awaiting confirmation."""


async def account_namespace(connection, instance, platform_name=None):
    if platform_name:
        return SHARED_NAMESPACES.get(platform_name, "instance:" + instance)
    cursor = await connection.execute(
        "SELECT account_namespace FROM account_platforms WHERE platform_instance_id=?", (instance,))
    row = await cursor.fetchone()
    return row[0] if row else "instance:" + instance


async def register_platform(connection, instance, platform_name):
    if instance in {"", "unknown"} or not platform_name:
        return
    namespace = SHARED_NAMESPACES.get(platform_name, "instance:" + instance)
    await connection.execute(
        "INSERT INTO account_platforms(platform_instance_id,account_namespace) VALUES (?,?) "
        "ON CONFLICT(platform_instance_id) DO UPDATE SET account_namespace=excluded.account_namespace "
        "WHERE account_namespace<>excluded.account_namespace", (instance, namespace))


async def routes_in_namespace(connection, namespace, account_id):
    cursor = await connection.execute(
        "SELECT * FROM account_routes WHERE account_namespace=? AND account_id=? ORDER BY platform_instance_id",
        (namespace, account_id))
    return [dict(row) for row in await cursor.fetchall()]


async def account_routes(connection, instance, account_id, platform_name=None):
    namespace = await account_namespace(connection, instance, platform_name)
    return await routes_in_namespace(connection, namespace, account_id)


async def resolve_owner(connection, instance, account_id, platform_name=None):
    namespace = await account_namespace(connection, instance, platform_name)
    routes = await routes_in_namespace(connection, namespace, account_id)
    # The first read can precede capture/registration for this instance. Include
    # any previously bound endpoint even if it has no recorded platform type yet.
    existing = next((route for route in routes if route["platform_instance_id"] == instance), None)
    if existing is None and platform_name:
        cursor = await connection.execute(
            "SELECT * FROM account_bindings WHERE platform_instance_id=? AND account_id=?", (instance, account_id))
        existing = await cursor.fetchone()
    if existing:
        return existing["logical_user_id"] if existing["binding_status"] == "active" else None
    if routes:
        owners = {route["logical_user_id"] for route in routes}
        if len(owners) > 1:
            raise AmbiguousAccountOwner()
        if all(route["binding_status"] == "active" for route in routes):
            return owners.pop()
        return None
    if namespace.startswith("instance:"):
        return default_logical_user_id(instance, account_id)
    return default_logical_user_id("social\x1f" + namespace, account_id)


async def delivery_owner(connection, instance, account_id, platform_name, observed_at, *, routes=None):
    """Resolve a confirmed delivery using the binding valid at its send time."""
    if routes is None:
        routes = await account_routes(connection, instance, account_id, platform_name)
    if not routes:
        return await resolve_owner(connection, instance, account_id, platform_name)
    owners = set()
    for route in routes:
        row = await (await connection.execute(
            "SELECT logical_user_id,binding_status FROM account_owner_history WHERE platform_instance_id=? "
            "AND account_id=? AND valid_from<=? ORDER BY valid_from DESC LIMIT 1",
            (route["platform_instance_id"], account_id, observed_at))).fetchone()
        if row and route["platform_instance_id"] == instance:
            return row["logical_user_id"] if row["binding_status"] == "active" else None
        if row and row["binding_status"] == "active":
            owners.add(row["logical_user_id"])
    if len(owners) > 1:
        raise AmbiguousAccountOwner()
    return next(iter(owners), None)


def group_accounts(rows):
    accounts = {}
    for row in rows:
        key = (row["logical_user_id"], row["account_namespace"], row["account_id"])
        if key not in accounts:
            accounts[key] = {**dict(row), "platform_instance_ids": []}
        accounts[key]["platform_instance_ids"].append(row["platform_instance_id"])
    return list(accounts.values())
