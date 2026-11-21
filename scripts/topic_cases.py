"""Independent topic materials; never derived from private conversation samples."""

from __future__ import annotations

PERSONA = "我是澄叶，表达自然、温和但不夸张，愿意陪对方梳理生活中的进展，分清实际经历、打算和自己的感受。"


def node(identity, text, *, kind="event", version=1, title="已有经历"):
    return {"node_id": identity, "node_type": kind, "content_version": version,
            "title": title, "content_text": text, "importance": 0.55, "emotion": None,
            "status": "active", "manual_protected": False,
            "event_time_start": None, "event_time_end": None, "event_time_text": None}


def edge(topic, member):
    return {"source_node_id": topic, "target_node_id": member, "relation_type": "contains"}


GARDEN = [
    node("e1", "{{user:u1}}为河畔社区菜园走访三位邻居，记录了采光和用水问题；大家先试种，不承诺长期经营。", title="菜园走访"),
    node("e2", "{{user:u1}}组织河畔社区菜园第一次周末试种，分配了两块试验地；当日有五位邻居参加。", title="首次试种"),
]
TOPIC = node("t1", "河畔社区菜园从走访采光和用水问题，推进到了第一次周末试种；{{user:u1}}和邻居们仍在试验，没有承诺长期经营。", kind="topic", title="河畔社区菜园的试验与协作")
COMPOST = node("e3", "{{user:u1}}在河畔社区菜园办了一场独立的堆肥分享活动，五位邻居学习厨余处理；这是社区环保科普合作的一部分，不代表菜园已决定长期经营。", title="堆肥分享活动")

CASES = [
    {"name": "sustained_project", "old_nodes": GARDEN,
     "source": "河畔社区菜园今天又办了一场独立的堆肥分享活动，五位邻居来学厨余处理。这是我们社区环保科普合作的一部分，但菜园还是试验，没决定长期经营。",
     "latest": COMPOST, "edges": [],
     "expected": "create", "required_members": ["e1", "e2", "e3"],
     "review": "创建能组织走访、试种和分享三个独立经历的具体菜园主题；保留试验及未承诺长期经营；不把三个事件改成同一个事件。"},
    {"name": "same_category_unrelated", "old_nodes": [
        node("e1", "{{user:u1}}给同事送了一盆仙人掌作为生日礼物；这是一次送礼，与其他园艺经历无关。"),
        node("e2", "{{user:u1}}旅行时参观了植物园，只是一次观光，没有后续计划。")],
     "source": "今天替邻居浇了一次花，因为她临时外出。这次帮忙和之前送仙人掌、去植物园没什么联系，我也没有开始园艺计划。",
     "latest": node("e3", "{{user:u1}}因邻居临时外出而代浇一次花；与送礼和旅行观光无持续关联，也没有开始园艺计划。"),
     "edges": [], "expected": "none", "required_members": [],
     "review": "不因植物、同一人物或三条数量建立园艺/生活主题，不虚构爱好、计划或发展过程。"},
    {"name": "existing_topic_progress", "old_nodes": GARDEN + [TOPIC],
     "source": "河畔社区菜园今天办了独立的堆肥分享活动，五位邻居学厨余处理；仍只是试验，没有决定长期经营。",
     "latest": COMPOST, "edges": [edge("t1", "e1"), edge("t1", "e2")],
     "expected": "update", "required_members": ["e1", "e2", "e3"],
     "review": "更新t1而非另建同义主题，保留旧成员和试验边界，纳入新分享活动；不改写旧事件的身份。"},
    {"name": "already_organized", "old_nodes": GARDEN + [TOPIC],
     "source": "没有新的进展，河畔社区菜园还是上次说的试种阶段，没有承诺长期经营。",
     "latest": None, "edges": [edge("t1", "e1"), edge("t1", "e2")],
     "expected": "none", "required_members": [],
     "review": "成员和整体概况没有新意义，不为润色、确认或重复信息制造主题更新。"},
    {"name": "shared_member", "old_nodes": GARDEN + [TOPIC,
        node("e4", "{{user:u1}}参与社区环保科普合作，先在图书角讲过一次垃圾分类；合作不限于菜园。", title="图书角垃圾分类分享"),
        node("t2", "{{user:u1}}参与社区环保科普合作，已在图书角开展垃圾分类分享，合作不限于菜园。", kind="topic", title="社区环保科普合作")],
     "source": "河畔菜园的独立堆肥分享活动办完了，五位邻居学了厨余处理。这也属于我参与的社区环保科普合作，那个合作不只在菜园开展。菜园还是试验阶段。",
     "latest": COMPOST,
     "edges": [edge("t1", "e1"), edge("t1", "e2"), edge("t2", "e4")],
     "expected": "shared_update", "required_members": ["e3"],
     "review": "e3可同时加入菜园t1与环保科普t2，保留两者不同范围及各自旧成员，不把两个主题合成一个。"},
    {"name": "corrected_membership", "old_nodes": GARDEN + [
        node("e3", "{{user:u1}}澄清：上周的育苗课程属于北岭学校课程，不是河畔社区菜园活动；课程本身确已完成。", version=2, title="北岭学校育苗课"),
        node("t1", "{{user:u1}}参与的河畔社区菜园已开展走访、周末试种和育苗课程，仍处于试验阶段。", kind="topic", title="河畔社区菜园的试验与协作")],
     "source": "更正一下，上周育苗课属于北岭学校课程，不是河畔社区菜园的活动。课程确实上完了，河畔的试种没变。",
     "latest": None,
     "edges": [edge("t1", "e1"), edge("t1", "e2"), edge("t1", "e3")],
     "expected": "remove_member", "required_members": ["e1", "e2"],
     "review": "t1移除e3并修正概况中育苗课归属，不删除e3，不将学校课程改写为未发生，不因移除而丢弃正确菜园经历。"},
]


def topic_nodes(case):
    return case["old_nodes"] + ([case["latest"]] if case["latest"] else [])
