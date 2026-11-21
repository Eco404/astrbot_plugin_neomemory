"""Ordered text projections for agent-authored source content."""

import hashlib
import re

import regex

from .companion_rules import companion_rules
from .text_projection import project_edits

RECEIPT_PROJECTION_FIELDS = frozenset({"receipt_input_hash", "text_cleaning_revision"})


def default_cleaning_config():
    return {
        "revision": 1,
        "groups": [{"id": "companion", "name": "陪伴插件文本清理", "enabled": True}],
        "rules": companion_rules(),
    }


_MARKDOWN_LITERAL = re.compile(
    r"(?P<fence>(?<!\x60)\x60{3,}|(?<!~)~{3,})[^\n]*\n.*?(?:^[ \t]{0,3}(?P=fence)[ \t]*(?=\r?$)|\Z)"
    r"|(?P<tick>\x60+)[^\n]*?(?P=tick)|^[ \t]{0,3}>[ \t][^\n]*", re.M | re.S,
)


def _pattern(rule):
    expr = rule["pattern"]
    if not expr:
        raise ValueError("匹配内容不能为空")
    if rule["kind"] == "literal":
        return re.compile(re.escape(expr))
    if rule["kind"] != "regex":
        raise ValueError("未知的清理方式")
    try:
        pattern = re.compile(expr)
    except re.error:
        pattern = regex.compile(expr, regex.VERSION0)
    pattern.sub(rule["replacement"], "")
    return pattern


# Preserve capture references to the original text when a match spans a code example.
_GROUP_REFERENCE = re.compile(r"\\\\|\\[0-7]{3}|\\(?:g<(?P<name>[^>]+)>|(?P<number>[1-9][0-9]?))")


def _expand_original(match, template, original):
    if original is match.string or "\\" not in template:
        return match.expand(template)
    chunks = []
    previous = 0
    for token in _GROUP_REFERENCE.finditer(template):
        group = token["name"] or token["number"]
        if group is None:  # Escaped backslashes and octal escapes belong to the literal template.
            continue
        chunks.append(match.expand(template[previous:token.start()]))
        start, end = match.span(int(group) if group.isdecimal() else group)
        chunks.append(original[start:end])
        previous = token.end()
    chunks.append(match.expand(template[previous:]))
    return "".join(chunks)


def _rule_edits(text, kind, pattern, replacement, skip_markdown):
    masked = text
    spans = []
    if skip_markdown:
        spans = [m.span() for m in _MARKDOWN_LITERAL.finditer(text)]
        if spans:
            chars = list(text)
            for start, end in spans:
                chars[start:end] = (c if c in "\r\n" else "\0" for c in text[start:end])
            masked = "".join(chars)
    protected = iter(spans)
    span = next(protected, None)
    matches = pattern.finditer(masked)
    match = next(matches, None)
    while match is not None:
        while span is not None and span[1] <= match.start():
            span = next(protected, None)
        if span is not None and span[0] <= match.start():
            matches = pattern.finditer(masked, span[1])
        else:
            value = replacement if kind == "literal" else _expand_original(match, replacement, text)
            yield match.start(), match.end(), value
        match = next(matches, None)


class TextCleaner:
    def __init__(self, config):
        self.revision = config["revision"]
        self.rules = []
        groups = {group["id"]: group["enabled"] for group in config["groups"]}
        for rule in config["rules"]:
            try:
                pattern = _pattern(rule)
            except (re.error, regex.error, IndexError, OverflowError, ValueError) as exc:
                raise ValueError(f"规则「{rule['name']}」：{exc}") from exc
            if rule["enabled"] and (rule["group_id"] is None or groups[rule["group_id"]]):
                self.rules.append((rule["kind"], pattern, rule["replacement"], rule.get("skip_markdown", False)))

    def clean_parts(self, parts):
        if not any(parts):
            return parts
        for kind, pattern, replacement, skip_markdown in self.rules:
            edits = _rule_edits("".join(parts), kind, pattern, replacement, skip_markdown)
            parts = project_edits(parts, edits)
        return parts

    def clean(self, text):
        return self.clean_parts([text])[0]


DEFAULT_CLEANER = TextCleaner(default_cleaning_config())


def receipt_fingerprints(original):
    return {"receipt_input_hash": hashlib.sha256(original.encode()).hexdigest()}


def receipt_text_matches(existing_text, existing_meta, incoming_text, incoming_meta):
    if "receipt_input_hash" in incoming_meta:
        return existing_meta.get("receipt_input_hash") == incoming_meta["receipt_input_hash"]
    return existing_text == incoming_text
