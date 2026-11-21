"""Ordered private-message attachments and their source-linked extraction evidence."""

import base64
import json
from collections import defaultdict

from .extraction_schema import _reject_constant, _unique_object

MAX_EVIDENCE_CHARS = 400
MAX_IMAGES = 10
_SCHEMA_MARKER = "\nJSON schema:\n"
_SUBJECT_RULE = (
    "按来源顺序阅读带署名的原话及紧随其后的附件。文字和上下文决定值得保留的经历，图像补充可见细节。"
    "写成记忆主人自己的记忆时，每个独立节点都要能看出谁做了什么。例如对方说‘我取了书’，写‘{{user:u1}}取了书’；"
    "对方说‘我替林姐传图，是她取的书’，写‘{{user:u1}}替林姐传图，书是林姐取的’；人格自己取书才写‘我取了书’。"
    "转发中的署名只说明图文出处，发图、代发、画面中的人和实际行动者应分别归属。图片不可读时只依据可用文字。"
)
_EVIDENCE_RULE = (
    "上述抽取字段放在 extraction 内。image_evidence 只列本次写入记忆确实用到的图片细节；没有采用时给空数组。"
    "逐图给出 source_id、slot 和简短 description，沿用对应图片的来源代号和原编号。description 只写画面或宿主图片描述支持的依据，"
    "保留数字、限定和不确定性，不混入发言者附言的更正或推断。已有保存依据无需再写。"
)


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def vision_schema(base_schema):
    """Wrap any selected extraction schema while preserving its root-relative refs."""
    schema = dict(base_schema)
    definitions = schema.pop("$defs", None)
    result = {
        "type": "object", "additionalProperties": False,
        "required": ["extraction", "image_evidence"],
        "properties": {
            "extraction": schema,
            "image_evidence": {
                "type": "array", "maxItems": MAX_IMAGES,
                "items": {
                    "type": "object", "additionalProperties": False,
                    "required": ["source_id", "slot", "description"],
                    "properties": {
                        "source_id": {"type": "string"},
                        "slot": {"type": "integer", "minimum": 1},
                        "description": {"type": "string", "minLength": 1, "maxLength": MAX_EVIDENCE_CHARS},
                    },
                },
            },
        },
    }
    if definitions:
        result["$defs"] = definitions
    return result


def _append_attachment(parts, item):
    label = {"source_id": item["source_alias"], "slot": item["slot"]}
    if item.get("sent_with_source"):
        label["同次发送的文字来源"] = item["sent_with_source"]
    elif item.get("sent_with_text"):
        label["随图发送的原话"] = item["sent_with_text"]
    if item.get("authors"):
        label["转发署名链"] = item["authors"]
    if item.get("evidence"):
        label["已保存的图片依据"] = item["evidence"]
        if item.get("evidence_origin"):
            label["依据来源"] = item["evidence_origin"]
    elif item.get("jpeg"):
        label["附件"] = "以下图片"
        parts.append({"type": "text", "text": _json(label)})
        encoded = base64.b64encode(item["jpeg"]).decode("ascii")
        parts.append({"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + encoded}})
        return
    elif item.get("caption"):
        label["宿主图片描述"] = item["caption"]
    else:
        label["图片状态"] = "不可读"
        if item.get("reason"):
            label["reason"] = item["reason"]
    parts.append({"type": "text", "text": _json(label)})


def vision_messages(messages, attachments, *, collect_evidence=True):
    """Assemble images beside their own quoted message; retries keep their feedback tail."""
    rules, schema_text = messages[0]["content"].rsplit(_SCHEMA_MARKER, 1)
    rules += "\n" + _SUBJECT_RULE
    if collect_evidence:
        rules += "\n" + _EVIDENCE_RULE
        schema_text = _json(vision_schema(json.loads(schema_text)))
    system = dict(messages[0], content=rules + _SCHEMA_MARKER + schema_text)
    header = json.loads(messages[1]["content"])
    sources = header.pop("sources")
    parts = [{"type": "text", "text": _json(header)}]
    by_source = defaultdict(list)
    for item in attachments:
        by_source[item["source_alias"]].append(item)
    for source in sources:
        source = dict(source)
        words = source.pop("content_text", "")
        if source["actor_kind"] == "agent":
            speaker = "记忆主人（正文里的我）"
        else:
            speaker = "{{user:" + source["actor_logical_user_id"] + "}}（私聊对象）"
        parts.append({"type": "text", "text": _json({"source": source, "发言者": speaker, "该发言者的原话": words})})
        for item in sorted(by_source[source["source_id"]], key=lambda item: item["slot"]):
            _append_attachment(parts, item)
    content = parts if any(part["type"] == "image_url" for part in parts) else "\n".join(p["text"] for p in parts)
    return [system, dict(messages[1], content=content), *messages[2:]]


class VisionOutputError(ValueError):
    code = "invalid_image_evidence"

    def __init__(self, path, instruction):
        super().__init__(instruction)
        self.path = path
        self.instruction = instruction


def _cited_sources(extraction):
    used = set()
    for field in ("node_changes", "profile_changes", "edges", "coverage_changes", "relationship_change"):
        changes = extraction.get(field)
        if isinstance(changes, dict):
            changes = [changes]
        if isinstance(changes, list):
            for change in changes:
                if isinstance(change, dict) and isinstance(change.get("source_ids"), list):
                    used.update(v for v in change["source_ids"] if isinstance(v, str))
    return used


def split_vision_output(raw, attachments):
    """Restore durable source IDs only for new evidence from an available, cited slot."""
    value = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    if not isinstance(value, dict) or set(value) != {"extraction", "image_evidence"} or not isinstance(value["extraction"], dict):
        raise VisionOutputError("$", "返回包含 extraction 对象和 image_evidence 数组的完整候选。")
    entries = value["image_evidence"]
    if not isinstance(entries, list) or len(entries) > MAX_IMAGES:
        raise VisionOutputError("$.image_evidence", f"图片依据应为数组，最多 {MAX_IMAGES} 项。")
    extraction = value["extraction"]
    cited = _cited_sources(extraction) if extraction.get("result") != "no_change" else set()
    available = {(item["source_alias"], item["slot"]): item for item in attachments
                 if not item.get("evidence") and (item.get("sha256") or item.get("jpeg") or item.get("caption"))}
    evidence = []
    for index, entry in enumerate(entries):
        path = f"$.image_evidence[{index}]"
        if (not isinstance(entry, dict) or set(entry) != {"source_id", "slot", "description"}
                or not isinstance(entry["source_id"], str) or type(entry["slot"]) is not int):
            raise VisionOutputError(path, "给出来源代号 source_id、原图片编号 slot 和文字依据 description。")
        item = available.pop((entry["source_id"], entry["slot"]), None)
        if item is None or entry["source_id"] not in cited:
            raise VisionOutputError(path, "只收录 extraction 已使用来源中确实采用的新图片依据；一图至多一项。")
        description = entry["description"]
        if not isinstance(description, str) or not description.strip() or len(description) > MAX_EVIDENCE_CHARS:
            raise VisionOutputError(path + ".description", f"给出 1～{MAX_EVIDENCE_CHARS} 字的图片依据。")
        evidence.append({"source_id": item["source_id"], "slot": item["slot"], "description": description.strip(),
                         "origin": "image" if item.get("jpeg") or item.get("sha256") else "host_caption"})
    return _json(extraction), evidence
