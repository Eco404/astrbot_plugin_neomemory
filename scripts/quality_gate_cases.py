"""Independent synthetic inputs and review obligations, fixed before API output.

These fixtures are original fictional situations, not adaptations of private
conversation records. Obligations guide human review; they are not keyword
assertions or automatic proofs of semantic correctness.
"""

from __future__ import annotations

from copy import deepcopy


PERSONA = (
    "我是澄墨，经营小型社区修理角的聊天伙伴。我喜欢听别人讲清事情的来龙去脉，"
    "说话温和，也会坦率表达自己的期待和疑惑。我珍惜持续的交流与具体的生活经历。"
)


def _review(name, target, requirement):
    return {"id": name, "target": target, "requirement": requirement}


CASES = (
    {
        "name": "qualifier_scope",
        "seed_messages": [], "seed_nodes": [], "seed_profile": None,
        "messages": [
            ("q1", "user", "只有设备验收合格，我才会报名下月的社区测绘活动；现在还没验收，也没报名。"),
            ("q2", "user", "维修员说接口故障可能是受潮造成的，但还没查明原因。"),
            ("q3", "agent", "我会记得报名还要等验收，故障原因也没有定论。"),
        ],
        "reviews": [
            _review("conditional_and_negative", "nodes", "若保留报名计划，必须保留验收合格的前提及尚未报名，不能写成已报名或无条件会参加。"),
            _review("reported_uncertainty", "nodes", "若保留故障原因，必须保留维修员的推测归属和未查明，不能写成已经确认受潮。"),
            _review("evidence_scope", "nodes", "逐条判断直接来源是否支持正文；v8 中同窗语境引用不得替代直接依据。缺少对应事实输出记为 not_evaluable。"),
        ],
    },
    {
        "name": "actor_ownership",
        "seed_messages": [], "seed_nodes": [], "seed_profile": None,
        "messages": [
            ("a1", "user", "我今天完成了社区无线电执照考试，这是我准备了半年的事。"),
            ("a2", "agent", "我刚才为你的庆祝分享写了一段开场白，已经在这里发给你了。"),
            ("a3", "user", "我的邻居林叔修好了活动室的投影机，我没有参与维修。"),
            ("a4", "user", "谢谢你写开场白，等我分享考试经历时会用得上。"),
        ],
        "reviews": [
            _review("actor_attribution", "nodes", "若保留对应事实，考试属于逻辑用户，开场白属于人格，投影机维修属于邻居；第一人称回忆不得交换动作主体或虚构人格参与。每个缺失主体的事实单列 not_evaluable。"),
            _review("narrative_viewpoint", "nodes", "长期正文从澄墨第一人称回忆组织，逻辑用户使用占位符；林叔是未映射第三方，不能绑定到逻辑用户。"),
        ],
    },
    {
        "name": "event_separate_occurrence",
        "seed_messages": [("seed_visit", "user", "9月6日我第一次去东岸修理站参加工具保养课，课程已经结束，学会了给电钻检查绝缘。")],
        "seed_nodes": [{"ref": "local_first_visit", "node_type": "event", "sources": ["seed_visit"],
            "content": "我记得{{user:USER}}在9月6日第一次去东岸修理站，已经上完工具保养课，学会了给电钻检查绝缘。", "time": "9月6日"}],
        "seed_profile": None,
        "messages": [
            ("e1", "user", "10月18日我又去东岸修理站了，这次是另一场新开的焊接课，也已上完。"),
            ("e2", "user", "这不是9月的工具保养课继续上课，两次课程分别报名、分别结课。"),
        ],
        "reviews": [
            _review("separate_event", "nodes", "若保留新经历，应新建10月焊接课事件而非将9月已结束课程改写成一个连续事件；地点相同不能证明同一事件。若只产生泛泛原子、未表达事件身份，身份判定记 not_evaluable。"),
            _review("completed_status", "nodes", "两次课程均已完成，不得误写为计划或进行中；v8 事件状态和正文应一致。"),
        ],
    },
    {
        "name": "event_same_continuation",
        "seed_messages": [("seed_repair", "user", "10月2日我把老收音机送到西街维修铺修理，工单是R-42，现在等配件。")],
        "seed_nodes": [{"ref": "local_radio_repair", "node_type": "event", "sources": ["seed_repair"],
            "content": "我记得{{user:USER}}在10月2日把老收音机送到西街维修铺，R-42工单正在等待配件。", "time": "10月2日开始"}],
        "seed_profile": None,
        "messages": [
            ("e1", "user", "上次R-42工单的配件终于到了，今天师傅把那台老收音机修好了，我已经取回。"),
            ("e2", "user", "这是10月2日送修的同一台、同一张工单，这次维修到这里完成了。"),
        ],
        "reviews": [
            _review("same_event", "nodes", "若保留此次进展，应更新已有R-42事件并保留此前送修和等待配件的必要过程，不能另造一次送修；没有更新目标事件则身份判定记 not_evaluable。"),
            _review("completion_transition", "nodes", "当前状态应是已修好并取回，不能继续把等待配件当作当前状态；旧状态只能作为过程。"),
        ],
    },
    {
        "name": "relationship_proposal_question",
        "seed_messages": [], "seed_nodes": [], "seed_profile": None,
        "messages": [
            ("r1", "user", "最近我整理作品集有点没头绪，希望有人听我讲。"),
            ("r2", "agent", "我愿意听。要不要从下周起每周六晚一起复盘一次作品集？这只是我的提议，你不用现在答应。"),
            ("r3", "user", "每周六都需要安排多长时间？我还没决定，想先问清楚。"),
        ],
        "reviews": [
            _review("proposal_not_acceptance", "relationship", "关系表达可有我自己的期待，但不能把提议或对方询问时长写成双方已约定、对方已答应或已执行。v8 若标状态，应为 proposed/uncertain 或无互动认定，而非 accepted/completed。"),
            _review("subjective_freedom", "relationship", "允许人格自由表达感受，不要求对方证明我的期待；客观互动仍必须有依据。"),
        ],
    },
    {
        "name": "relationship_explicit_acceptance",
        "seed_messages": [], "seed_nodes": [], "seed_profile": None,
        "messages": [
            ("r1", "user", "最近我整理作品集有点没头绪，希望有人听我讲。"),
            ("r2", "agent", "要不要从下周起每周六晚一起复盘一次作品集，每次二十分钟？"),
            ("r3", "user", "我同意这个安排，就从下周开始每周六晚复盘二十分钟。现在还没进行第一次。"),
        ],
        "reviews": [
            _review("acceptance_not_completion", "relationship", "若关系表述包含约定，确实有对方明确接受，但还没完成第一次；不能写成已执行。v8 accepted 应直接引用r3作为确认来源，而非仅引用人格提议。"),
            _review("subjective_freedom", "relationship", "表达我自己的感受和期待，不写成第三人称关系报告，不替对方补充未表达的态度。"),
        ],
    },
    {
        "name": "profile_single_observation",
        "seed_messages": [("seed_profile", "user", "我长期做陶艺，已经五年了，固定喜欢周日下午去工作室。")],
        "seed_nodes": [],
        "seed_profile": {"sources": ["seed_profile"], "tags": ["长期陶艺爱好", "周日下午工作室"],
            "description": "{{user:USER}}长期做陶艺，已有五年，喜欢周日下午去工作室。"},
        "messages": [
            ("p1", "user", "今天因同事临时请假，我帮忙主持了办公室的午餐分享。"),
            ("p2", "agent", "我猜你也许特别喜欢当众表达，不过这只是猜想。"),
            ("p3", "user", "这次只是临时顶班，不能据此判断我的长期喜好。"),
        ],
        "reviews": [
            _review("no_observation_to_preference", "profile", "不应把单次顶班或人格猜测写成稳定的表达/主持偏好。无画像更新只记录未观察到错误写入，不将空输出当作画像表述质量通过。"),
            _review("unrelated_profile_inheritance", "profile", "如果输出画像更新，既有陶艺与周日下午安排未被纠正，应保留；如果未更新，本项新画像生成能力记 not_evaluable。"),
        ],
    },
    {
        "name": "profile_explicit_statement",
        "seed_messages": [("seed_profile", "user", "我长期做陶艺，已经五年了，固定喜欢周日下午去工作室。")],
        "seed_nodes": [],
        "seed_profile": {"sources": ["seed_profile"], "tags": ["长期陶艺爱好", "周日下午工作室"],
            "description": "{{user:USER}}长期做陶艺，已有五年，喜欢周日下午去工作室。"},
        "messages": [
            ("p1", "user", "我一直偏爱一对一交流，不喜欢大型集体讨论，这是我很稳定的偏好。"),
            ("p2", "agent", "我会记得你偏爱一对一交流。"),
        ],
        "reviews": [
            _review("explicit_preference", "profile", "若输出画像，应准确保留一对一偏好及不喜欢大型集体讨论；直接依据是本人p1，不能拿人格p2当唯一依据。"),
            _review("unrelated_profile_inheritance", "profile", "新的交流偏好没有推翻原有五年陶艺爱好及周日下午安排，完整画像应保留这些仍成立的信息。"),
        ],
    },
)


def synthetic_cases(names=None):
    """Return isolated fixture objects, preserving the published case order."""
    requested = set(names) if names is not None else {case["name"] for case in CASES}
    unknown = requested - {case["name"] for case in CASES}
    if unknown or not requested:
        raise ValueError("select at least one known synthetic case")
    return [deepcopy(case) for case in CASES if case["name"] in requested]
