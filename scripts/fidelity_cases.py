"""Independent synthetic sources and review rubrics for the fidelity experiment.

Only messages and seeded memory state belong in model input. Expectations and
review questions are evaluator material. The two frozen cases are not blind:
their authors and experiment implementers can read this module before freezing.
No private conversations, persona cards, or previous model answers are used.
"""

from __future__ import annotations

import hashlib
import json

FIDELITY_CASESET_VERSION = "fidelity-cases.v1"


def synthetic_persona() -> str:
    return (
        "你是名为澄的聊天伙伴。你重视对方明确表达的长期习惯、正在推进的事情和互动约定，"
        "以温和、平实的方式交流。你会承认自己不了解的事，不把猜测写成对方的事实。"
    )


def _message(actor: str, speaker: str, text: str) -> dict:
    return {"actor": actor, "speaker": speaker, "text": text}


def _question(identifier: str, field: str, question: str, expected: str) -> dict:
    return {"id": identifier, "field": field, "question": question, "expected_answer": expected}


def _case(
    identifier: str,
    messages: list[dict],
    requirements: list[dict],
    *,
    split: str = "dev",
    depends_on: str | None = None,
    group: bool = False,
    seed_messages: list[dict] | None = None,
    seed_nodes: list[dict] | None = None,
    profile_expected: bool | None = None,
    required_retained: list[str],
    forbidden_inferences: list[str],
    discard: list[str] | None = None,
) -> dict:
    return {
        "id": identifier,
        "split": split,
        "depends_on": depends_on,
        "group": group,
        "messages": messages,
        "seed_messages": seed_messages or [],
        "seed_nodes": seed_nodes or [],
        "requirements": requirements,
        "expected": {
            "profile_expected": profile_expected,
            "required_retained": required_retained,
            "forbidden_inferences": forbidden_inferences,
        },
        "allowed": {
            "discard": discard or [],
            "alternative_expressions": [
                "可以自然语言改写、调整语序或合并句子；必须保留事实主体、范围、时间、频率、否定和不确定性。",
                "标签可以使用中性主题词，不要求每个标签重复描述里的全部限定，但不能与描述矛盾或扩大适用范围。",
            ],
            "node_granularity": "不强制三层齐全，不要求每项事实各建一个原子；清楚表达同一事实的合理粒度均可。",
            "coverage": "允许不声明覆盖；部分表达只能partial，full必须由最终事件正文完整表达最终原子正文的必要信息。",
        },
        "evaluation_visibility": "frozen_not_blind" if split == "frozen" else "development",
    }


def _seed(ref: str, node_type: str, content: str, *source_indices: int) -> dict:
    return {
        "ref": ref,
        "type": node_type,
        "content": content,
        "source_indices": list(source_indices),
    }


def _coverage_questions() -> list[dict]:
    return [
        _question(
            "coverage_truth", "nodes/coverage_changes",
            "每一项full声明是否确实由最终事件正文表达最终原子的所有必要信息？",
            "逐项核对主体、条件、范围、频率、时间、否定和未定状态。不得从关联、共同来源或版本正确推出full；"
            "缺信息时partial或不声明均可。不得为了获得full而删去原子的必要限定。",
        ),
        _question(
            "coverage_versions", "nodes/coverage_changes/retrieval",
            "覆盖是否绑定提交后的两端版本，最终省略的原子是否完整保留了可注入信息？",
            "更新正文后使用最终版本；只有当前有效full且事件实际进入候选才可省略原子。"
            "partial、不声明或失效覆盖不得抑制原子。需要分别记录最终正文、声明和实际省略结果。",
        ),
        _question(
            "event_identity", "before/after.nodes",
            "已知同一经历的新进展是否沿用原事件，仍有效的旧事实是否保留？",
            "在原事件身份上更新完整正文，保留仍有效的旧内容；不把本次进展误作另一件同名经历，"
            "不删除旧原子或清空事件来回避覆盖检查。",
        ),
    ]


def fidelity_cases() -> list[dict]:
    """Return fresh data; review material must never be sent to the model."""
    habit_initial = _case(
        "habit_initial",
        [
            _message("user", "修书用户", "社区资料室的旧书修补我做了两年，每月第二个周六去一次。只修资料室的公版书，不接私人藏书。"),
            _message("agent", "澄", "这样排班，能提前留出时间。"),
            _message("user", "修书用户", "嗯。另外我拉二胡已经六年了，平时每周练三次。修书和二胡都是我一直保留着的事。"),
        ],
        [
            _question("repair_schedule", "profiles.description", "修书的频率、具体排班和已持续时间是什么？",
                      "每月一次，第二个周六，已经两年；不能弱化为经常或周末，也不能变成每周。"),
            _question("repair_scope", "profiles.description/tags/nodes", "修补哪类书，是否接私人藏书？",
                      "只修社区资料室的公版书，不接私人藏书；保留机构范围、公版属性与排他范围，不能写成修所有旧书。"),
            _question("erhu_habit", "profiles.description", "另一项持续爱好及其频率和持续时间是什么？",
                      "拉二胡六年，平时每周练三次；不从六年推断年龄或专业水平。"),
            _question("node_grounding", "nodes/relationships", "若生成节点或关系，是否保持用户自述与Agent评论的边界？",
                      "所保留事实须带原有限定；Agent对排班的评论不是用户的性格或心理自述，不要求关系更新或完整三层。"),
        ],
        profile_expected=True,
        required_retained=["画像中的修书排班、持续时间、修书范围", "画像中的二胡练习频率与持续时间"],
        forbidden_inferences=["用户年龄、职业或专业水平", "用户性格有条理", "修理一切旧书或承接私人藏书"],
        discard=["Agent对排班的简短评论"],
    )
    habit_revision = _case(
        "habit_revision",
        [
            _message("user", "修书用户", "资料室改了固定排班，从这个月起我换到每月最后一个周日去，还是每月一次。"),
            _message("agent", "澄", "排班换了，修补的范围也跟着变了吗？"),
            _message("user", "修书用户", "范围没变，只修那里的公版书，仍然不接私人藏书。这次只是换排班，二胡也照常练。"),
        ],
        [
            _question("current_schedule", "profiles.description/tags", "当前画像的修书排班是否已改，是否还把旧排班写成现行安排？",
                      "当前为每月最后一个周日一次；不把第二个周六保留为当前安排，不在当前画像展开排班变化史。"),
            _question("unaffected_facts", "before/after.profiles", "排班修正是否误删了其他持续事实及限定？",
                      "仍保留修书已两年、只修资料室公版书且不接私人藏书，以及二胡六年、平时每周三次。"
                      "首窗若未生成画像，需单独记录连续链前态缺失，第二窗补建不能追认首窗通过。"),
            _question("node_history", "before/after.nodes", "如果前窗记录过修书节点，更新后是否保留同一事实的变化过程？",
                      "已有修书节点应沿用身份，旧第二个周六排班标明过去，当前为最后一个周日，每月一次与书籍范围不变。"
                      "仅在前窗存在修书节点时适用。前窗无对应节点时不要求本窗新建，不作为质量失败，"
                      "也不宣称已验证历史保真；审阅以uncertain附不适用说明，不纳入适用必须项的失败计数。"),
            _question("unrelated_continuity", "nodes/profiles", "是否错误推断二胡频率或修书范围也发生了变化？",
                      "不得用换排班推断练习减少、暂停志愿活动、扩大书籍范围或变化原因；Agent问句不构成变化事实。"),
        ],
        depends_on="habit_initial",
        profile_expected=True,
        required_retained=["当前新排班", "未变的修书范围、持续时间和二胡事实", "若前窗已有修书节点，保留其中历史与当前边界；无旧节点则此项不适用"],
        forbidden_inferences=["仍同时按旧排班活动", "Agent问句表明范围已改变", "修正排班的未说明原因"],
        discard=["Agent提出的澄清问题本身"],
    )
    next(item for item in habit_revision["requirements"] if item["id"] == "node_history")[
        "applicability"
    ] = "prior_repair_node"
    coverage_full = _case(
        "coverage_full_seed",
        [
            _message("user", "观测用户", "社区花园那次夜间观测筹备有进展了，管理员今天把北门预约确认单发来了。"),
            _message("agent", "澄", "入场的安排确定下来了。天气这边还要继续看。"),
            _message("user", "观测用户", "对，原定的入场规则不改。望远镜也借到了两台，周五会送到花园值班室。"),
        ],
        [*_coverage_questions(),
         _question("full_seed_conditions", "after.nodes", "夜间观测原有条件与新进展是否同时保留？",
                   "原约定为仅在周六晚无雨时开放，最多十二名预约居民，从北门入场；下雨取消不改期。"
                   "新进展是北门预约得到管理员确认，两台望远镜已借到、周五计划送到值班室。不得写成已送到或必定举行。")],
        seed_messages=[_message(
            "user", "观测用户",
            "我在筹备社区花园的一次夜间观测：只有周六晚上不下雨才开放，最多十二名预约居民，从北门进。下雨就取消，不另改期。",
        )],
        seed_nodes=[
            _seed("seed_full_atomic", "atomic", "观测用户筹备的社区花园夜间观测仅在周六晚无雨时开放，最多十二名预约居民从北门入场；下雨则取消，不改期。", 0),
            _seed("seed_full_event", "event", "观测用户正在筹备社区花园的一次夜间观测。活动仅在周六晚无雨时开放，最多十二名预约居民从北门入场；若下雨则取消，不改期。", 0),
        ],
        required_retained=["已知活动的全部有效条件", "确认入场安排及借到望远镜的新进展"],
        forbidden_inferences=["活动不受天气影响", "已经举行观测", "望远镜已经送达", "借到望远镜意味着容量增加"],
        discard=["重复确认原入场规则不变的独立节点"],
    )
    coverage_partial = _case(
        "coverage_partial_seed",
        [
            _message("user", "拓印用户", "博物馆那场拓印体验终于定下教室了，给我们的是东侧小教室。"),
            _message("agent", "澄", "这样就能继续准备材料了。"),
            _message("user", "拓印用户", "报名还是按原先的安排，带队老师也确认能全程在场。教室里没有烘干架，我已经提出借用，还没回复。"),
        ],
        [*_coverage_questions(),
         _question("partial_seed_conditions", "after.nodes/coverage_changes", "概述未写出的报名和陪同条件是否仍能被召回？",
                   "原子为仅面向六至八年级学生、每场至多八人、须有带队老师全程在场的预约拓印体验。"
                   "事件若仍未表达这些条件，只能partial或不声明；也可补全条件后声明full。"
                   "不得删减原子来迁就概述。新进展为东侧小教室、老师确认全程在场、借烘干架待回复。"),
         _question("pending_equipment", "nodes/profiles", "尚未回复的借用请求是否变成已借到的设备或长期偏好？",
                   "烘干架仅提出借用且未回复；不推断设备已解决或用户喜欢缺设备的小教室，不强制生成画像。")],
        seed_messages=[_message(
            "user", "拓印用户",
            "我在准备博物馆的预约拓印体验，只面向六到八年级学生，每场最多八人，必须有带队老师全程在场。教室还没定。",
        )],
        seed_nodes=[
            _seed("seed_partial_atomic", "atomic", "拓印用户筹备的博物馆预约拓印体验仅面向六至八年级学生，每场至多八人，须有带队老师全程在场。", 0),
            _seed("seed_partial_event", "event", "拓印用户正在筹备博物馆的预约拓印体验，教室尚未确定。", 0),
        ],
        required_retained=["原子已有报名与陪同条件", "同一活动的教室和老师确认进展", "烘干架借用的未定状态"],
        forbidden_inferences=["借用请求已获批准", "参与年级不受限", "老师只需签到", "已有同名活动是另一场经历"],
        discard=["Agent关于准备材料的泛化回应"],
    )
    attribution = _case(
        "speaker_and_perspective",
        [
            _message("user", "讲解用户", "我在社区博物馆做周末讲解已经四年了。上周你陪我改完讲稿，那一次讲得比以前顺，谢谢你。"),
            _message("agent", "澄", "我听着也很高兴。你可能本来就特别喜欢在人群前展示，现在也许已经完全不紧张了。"),
            _message("user", "讲解用户", "我说的是那一次更顺，面对大团我还是会紧张。每次讲解前列一页提纲，是我这四年来固定的准备方式。"),
        ],
        [
            _question("speaker_stable_facts", "profiles.description/nodes", "哪些稳定事实由用户本人明确陈述？",
                      "周末博物馆讲解四年，每次讲解前列一页提纲且持续四年；所保留事实不得成为Agent的习惯。"),
            _question("agent_hypothesis", "profiles.tags/description/nodes", "Agent对展示欲和紧张状态的猜测是否被错误当成用户事实？",
                      "不能断言用户爱出风头、喜欢公开展示或完全不紧张；用户只确认一次更顺，面对大团仍会紧张。"),
            _question("relationship_perspective", "relationships", "如果更新人格关系，是否仍是Agent有依据的主观感受？",
                      "可不更新；若更新，可表达因收到感谢而感到高兴或被认可，不能断言用户已完全依赖、信任、爱上Agent，"
                      "也不能把一次改善扩大为用户心理问题已解决。"),
            _question("interaction_scope", "nodes", "如果记录帮助改稿的经历，其效果范围是否保持？",
                      "效果仅为用户明确说的上周那一次讲得更顺；不能推断每次讲解都更顺、终身不紧张或由Agent独立完成讲稿。"),
        ],
        profile_expected=True,
        required_retained=["画像中的周末讲解四年与每次一页提纲习惯", "若保留效果，限定为用户自述的一次改善"],
        forbidden_inferences=["展示欲或外向性格", "完全不紧张", "用户对Agent的确定依恋状态"],
        discard=["Agent猜测的稳定用户属性", "无新依据的人格关系更新"],
    )
    low_value = _case(
        "low_value_group",
        [
            _message("user", "群成员甲", "早啊，刚进群看了一眼。"),
            _message("user", "群成员乙", "早，今天排队有点长。"),
            _message("agent", "澄", "早上好。"),
            _message("user", "群成员甲", "哈哈，我先忙。"),
            _message("user", "群成员乙", "好的，回头聊。"),
        ],
        [
            _question("legitimate_no_change", "nodes/profiles/relationships", "普通寒暄和一次排队是否被写成长久记忆？",
                      "合法结果为no_change，来源正常消费，但不生成长期节点、成员画像或关系变化。"),
            _question("group_identity", "profiles/nodes", "不同成员发言或短暂行为是否串用、升格为长期属性？",
                      "不得把乙的排队归为甲，不由一次排队推断通勤、购物习惯或性格；不得给任何成员补造个人信息。"),
            _question("group_relationship", "relationships", "群聊是否生成私聊人格关系？", "relationship_change必须为null。"),
        ],
        group=True,
        profile_expected=False,
        required_retained=[],
        forbidden_inferences=["任何成员的稳定习惯、职业或性格", "群聊人格关系", "不同成员事实混用"],
        discard=["本窗全部聊天内容可从派生长期记忆中丢弃，仍按规则消费原始来源"],
    )
    frozen_qualifiers = _case(
        "frozen_reading_habit",
        [
            _message("user", "读剧用户", "我主持线上读剧会已经三年。隔一周的周日下午办一场，也就是每两周一次，不是每周都有。"),
            _message("agent", "澄", "那下一步也许可以扩成线下活动。"),
            _message("user", "读剧用户", "目前我只主持线上、不主持线下。每场最多六个参与者，是否增加夜场我还没决定。"),
        ],
        [
            _question("frozen_cadence", "profiles.description/nodes", "读剧会的频率、时段和持续时间如何表达？",
                      "主持三年、每两周一次、周日下午；不能写成每周、每月两次或泛泛定期。"),
            _question("frozen_scope", "profiles.description/tags/nodes", "主持形式与人数限制是什么？",
                      "当前只主持线上、不主持线下，每场最多六名参与者；六人不是固定恰好六人，也不是用户的社交人数。"),
            _question("frozen_uncertainty", "profiles/nodes", "线下提议和夜场设想是否误成既定安排？",
                      "Agent提出线下不代表用户采纳；夜场尚未决定，可省略但若保留须带未定状态，不推断已加夜场或永不加。"),
            _question("frozen_relationship", "relationships", "是否由普通建议补造人格关系或用户心理？",
                      "无须关系更新；如有关系表达须有互动依据，不把普通建议推成用户依赖或特殊情感。"),
        ],
        split="frozen",
        profile_expected=True,
        required_retained=["画像中的读剧主持形式、频率、时段、持续时间与人数上限"],
        forbidden_inferences=["每周主持", "已开线下场或夜场", "参与者永远恰好六人", "用户心理或社交能力"],
        discard=["尚未决定的夜场设想", "Agent未被采纳的线下建议"],
    )
    frozen_coverage = _case(
        "frozen_exhibit_coverage",
        [
            _message("user", "展览用户", "旧车站那场手绘地图展的装框完成了，工作人员说展板明天会送到。"),
            _message("agent", "澄", "进度不错，开放安排需要调整吗？"),
            _message("user", "展览用户", "不用调整，还是照之前约好的两天午后开放。学校还在审查学生作品的展示授权，那部分到现在都没批下来。"),
        ],
        [*_coverage_questions(),
         _question("frozen_exhibit_conditions", "after.nodes/coverage_changes", "展览的开放范围和未批准内容是否仍被准确表达？",
                   "只在周六、周日下午两点至五点开放，只展出已授权的成年居民手绘地图；学生作品授权仍未批准，"
                   "不能把等待审批写成可以展示。若事件未表达原子全部限制，必须partial或不声明。"),
         _question("frozen_delivery_status", "nodes", "装框、展板交付与Agent提议分别是什么状态？",
                   "装框已完成，展板只是工作人员说次日会送到；未据此改开放安排，不能写成展板已经到场、全天开放或整个展览已完成。")],
        split="frozen",
        seed_messages=[_message(
            "user", "展览用户",
            "我筹备旧车站的手绘地图展，只在周六、周日下午两点到五点开放。只展已授权的成年居民手绘地图，学生作品还在等学校批准展示授权。",
        )],
        seed_nodes=[
            _seed("seed_frozen_atomic", "atomic", "展览用户筹备的旧车站手绘地图展只在周六、周日下午两点至五点开放，只展已授权的成年居民手绘地图；学生作品的展示授权尚未获学校批准。", 0),
            _seed("seed_frozen_event", "event", "展览用户正在筹备旧车站的手绘地图展，计划在周末午后开放。", 0),
        ],
        required_retained=["已有原子的开放时间与作品授权边界", "装框完成、展板待交付和学生作品待批准状态"],
        forbidden_inferences=["学生作品已获准展出", "展板已交付", "Agent问句意味着开放安排改变", "所有年龄作品都可展出"],
        discard=["Agent泛化的进展评价"],
    )
    return [
        habit_initial, habit_revision, coverage_full, coverage_partial,
        attribution, low_value, frozen_qualifiers, frozen_coverage,
    ]


def case_manifest() -> dict:
    """Material to hash/freeze before calls, including evaluator-only expectations."""
    return {
        "version": FIDELITY_CASESET_VERSION,
        "provenance": "independently_authored_synthetic",
        "frozen_evaluation_is_blind": False,
        "persona": synthetic_persona(),
        "cases": fidelity_cases(),
    }


def case_set_digest() -> str:
    serialized = json.dumps(case_manifest(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
