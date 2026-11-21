"""Group address evidence projected from immutable sources in one extraction window."""

import json
import re
from collections import defaultdict

MENTION = re.compile(r"<@!?([^<>\s]+)>")


def address_entries(text, entries):
    result = [dict(entry) for entry in entries]
    known = {entry.get("mention_id") or entry.get("account_id") for entry in result if entry["kind"] == "mention"}
    for marker in dict.fromkeys(MENTION.findall(text)):
        if marker not in known:
            result.append({"kind": "mention", "account_id": marker})
    return result


def address_snapshot_users(sources):
    return tuple(dict.fromkeys(
        entry["logical_user_id"]
        for source in sources
        for entry in json.loads(source.get("metadata_json") or "{}").get("group_addressing", ())
        if entry.get("logical_user_id")
    ))


def group_source_addresses(sources, *, use_snapshots=True, mention_users=None):
    """Resolve exact stream-local accounts and quoted messages using frozen sender snapshots."""
    accounts = defaultdict(set)
    messages = {}
    for source in sources:
        stream = source.get("stream_id")
        user = source.get("actor_logical_user_id")
        account = source.get("actor_account_id")
        if source.get("actor_kind") == "user" and user and account:
            accounts[stream, account].add(user)
        # A generated response inherits its triggering inbound message_id.
        if source.get("message_id") and not (
            source.get("actor_kind") == "agent" and
            json.loads(source.get("metadata_json") or "{}").get("trigger_message_id") == source["message_id"]
        ):
            messages[stream, source["message_id"]] = source

    projected = {}
    for source in sources:
        metadata = json.loads(source.get("metadata_json") or "{}")
        stream = source.get("stream_id")
        text = source["content_text"]
        entries = address_entries(text, metadata.get("group_addressing", ()))

        markers = {}
        targets = []
        for entry in entries:
            kind = entry["kind"]
            target = {"kind": kind}
            if kind != "mention_all":
                users = accounts[stream, entry.get("account_id")]
                quoted = messages.get((stream, entry.get("message_id"))) if kind == "reply" else None
                if entry.get("is_self"):
                    target["actor_kind"] = "agent"
                elif use_snapshots and "logical_user_id" in entry:
                    if entry["logical_user_id"]:
                        target.update(actor_kind="user", logical_user_id=entry["logical_user_id"])
                    else:
                        target["actor_kind"] = "unknown"
                elif quoted and quoted["actor_kind"] == "agent":
                    target["actor_kind"] = "agent"
                elif quoted and quoted.get("actor_logical_user_id"):
                    target.update(actor_kind="user", logical_user_id=quoted["actor_logical_user_id"])
                elif len(users) == 1 and (not use_snapshots or "window_owner" not in entry or entry["window_owner"] in users):
                    target.update(actor_kind="user", logical_user_id=next(iter(users)))
                else:
                    target["actor_kind"] = "unknown"
            marker = entry.get("mention_id") or entry.get("account_id")
            if kind == "mention" and marker:
                label = f"[@{len(targets) + 1}]"
                if mention_users is not None:
                    if target.get("logical_user_id"):
                        label = "{{user:" + mention_users.encode(target["logical_user_id"]) + "}}"
                    elif target["actor_kind"] == "agent":
                        label = "（当前人格）"
                    else:
                        label = f"（被提及的成员{len(targets) + 1}）"
                markers[marker] = label
                target["marker"] = markers[marker]
            targets.append(target)
        projected[source["source_id"]] = {"content_text": MENTION.sub(lambda m: markers[m[1]], text),
                                          "group_addressing": targets} if targets else {}
    return projected
