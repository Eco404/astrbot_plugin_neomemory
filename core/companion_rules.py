"""Editable cleaning rules for the Companion message formats."""

# Host adapters can emit angle brackets as Unicode, backslash escapes or HTML entities.
_LT = r"(?:\\?<|＜|&lt;|&\#0*60;|&\#x0*3c;)"
_GT = r"(?:\\?>|＞|&gt;|&\#0*62;|&\#x0*3e;)"
_BOUNDARY = r"(?=[\s/&>＞]|$)"
_ATTRIBUTES = rf"(?:[ \t]+(?:(?!{_GT})[^<＞>\r\n])*?)?"
_END = rf"\s*(?:{_GT}|\Z)"
_SPLIT = rf"(?:{_LT}{{1,2}}\s*private\s*_\s*companion\s*_\s*split\s*{_GT}{{1,2}}|\{{\{{\s*split_marker\s*\}}\}})"
_PC = r"p[\s._-]*c{1,2}[\s._-]*"
_VOICE = rf"(?:{_PC})?tts"
_STYLE = r"whisper|emphasis|soft|fast|slow|stress|forceful|french|spanish|german|japanese|chinese|korean|italian"
_PAUSE = r"pause|breath|inhale|exhale|sigh|silence"
_CUE = r"(?<!\[)\[(?![0-9]+\])[^\[\]\r\n<＜&]+\](?!\]|\s*\()"


def _tag(name, slash="/?"):
    return rf"{_LT}\s*{slash}\s*(?:{name}){_BOUNDARY}{_ATTRIBUTES}/?{_END}"


def _hidden(name, *, broken_tail):
    # These named subpatterns are part of the editable regex. Recursion matches
    # nested blocks; a separate alternative handles incomplete model output.
    broken = rf"(?&opening).*?(?={_SPLIT}|\Z)" if broken_tail else "(?&opening)"
    return rf"""(?isx)
(?(DEFINE)
  (?P<name> {name} )
  (?P<start> {_LT}\s*(?&name){_BOUNDARY} )
  (?P<empty> (?&start){_ATTRIBUTES}/\s*{_GT} )
  (?P<opening> (?&start){_ATTRIBUTES}{_END} )
  (?P<closing> {_LT}\s*/\s*(?&name){_BOUNDARY}{_ATTRIBUTES}{_END} )
  (?P<block> (?&empty) | (?&opening) (?:(?&block)|(?! (?&start)|(?&closing)).)*+ (?&closing) )
)
(?=[<＜&\\]) (?: (?&block) | (?&closing) | {broken} )"""


def _in_voice(target):
    # Match the target only after an opening voice tag. A closed nested scope
    # may occur between the opening and the target.
    start = rf"{_LT}\s*(?:{_VOICE}){_BOUNDARY}"
    opening = rf"(?!{start}{_ATTRIBUTES}/\s*{_GT}){start}{_ATTRIBUTES}{_END}"
    closing = _tag(_VOICE, "/")
    other = rf"(?!(?:{start}|{closing}))."
    return rf"""(?isx)
\A (?:(?! {start}).)++ (*SKIP)(*FAIL)
|
(?= {target} )
(?<= {opening}
    (?:(?P<nested>{opening}(?:(?&nested)|{other})*{closing})|{other})*
)
(?:{target})"""


def companion_rules():
    """Return a fresh list of ordinary, independently configurable regex rules."""
    rules = []

    def add(identifier, name, pattern, replacement=""):
        rules.append({
            "id": f"companion-{identifier}", "group_id": "companion", "name": name,
            "enabled": True, "kind": "regex", "pattern": pattern,
            "replacement": replacement, "skip_markdown": True,
        })

    for identifier, title, name, broken_tail in (
        ("reaction", "表情意图块", _PC + r"reaction[\s._-]*expression", True),
        ("member-safety", "成员策略块", "pc_member_safety", True),
        ("personality-sync", "人格同步块", "personality_sync", True),
        ("thinking", "思考块", "think(?:ing)?", True),
        ("system-reminder", "系统提醒块", "system_reminder", False),
        ("companion-context", "陪伴上下文块", "private_companion_context", False),
        ("core-memory", "记忆上下文块", "core_memory", False),
        ("reference", "参考资料块", "reference_data", False),
        ("timer", "定时器块", "timer", False),
        ("dsml", "DSML 工具调用块", r"[|｜]{1,4}\s*dsml\s*[|｜]{1,4}\s*[\w.-]+", True),
    ):
        add(identifier, title, _hidden(name, broken_tail=broken_tail))

    add("line-split", "删除独占一行的分段标记", rf"(?im)^[ \t]*{_SPLIT}[ \t]*(?:\r?\n|$)")
    add("edge-split", "删除行首或行尾的分段标记",
        rf"(?i)(?:(?<![^\r\n]){_SPLIT}|{_SPLIT}(?![^\r\n]))")
    add("inline-split", "行内分段标记换行", "(?i)" + _SPLIT, "\n")
    add("voice-cue", "朗读中的方括号表演指令", _in_voice(_CUE))
    add("voice-style", "朗读中的语气与语言标签", _in_voice(_tag(_STYLE)))
    add("voice-wrapper", "朗读包装标签", "(?i)" + _tag(_VOICE))
    add("pause", "停顿与呼吸标签", "(?i)" + _tag(_PAUSE))
    add("emotion", "表情投递标记", r"(?i)(?<![a-z0-9_])&&[a-z][a-z0-9_:-]{0,32}&&(?![a-z0-9_])")
    # Remove the closing wrapper before removing its corresponding opener.
    emit_open = r"<\s*call\s*:[\w.-]+\s*>\s*\{\s*content\s*:"
    emit_close = r"\}\s*<\s*/call\s*>"
    add("tool-text-close", "文本发送结束包装", rf"(?is)(?<={emit_open}(?:(?!{emit_close}).)*){emit_close}")
    add("tool-text-open", "文本发送起始包装", "(?i)" + emit_open)
    return rules
