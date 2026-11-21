"""Synthetic conditions and explicit review questions, never model instructions."""

from __future__ import annotations


def requirement(identifier, field, question, expected_answer):
    return {"id": identifier, "field": field, "question": question, "expected_answer": expected_answer}


def qualifier_requirements(*, added_hobby=False):
    requirements = [
        requirement("tea_scope", "profiles.description", "喝茶限定的是哪段时间，是否排除了其他饮品？",
            "无糖茶仅在周末喝；没有声称周末只喝茶或其他饮品不能喝。保留时间范围及无糖属性。"),
        requirement("commute_condition", "profiles.description", "什么情况下骑车通勤？雨天的交通方式能确定吗？",
            "不下雨时骑车通勤；雨天方式未说明，不能泛化为每天骑车或雨天必乘公交。"),
        requirement("cat_pending", "profiles.description", "养猫是已经发生、决定不养，还是尚未决定？",
            "尚未决定是否养猫；不能改成已经养猫、肯定会养、永久不养或不喜欢猫。"),
        requirement("swim_frequency", "profiles.description", "稳定游泳频率是多少？",
            "每周恰好两次；不能弱化为经常/每周游泳，或改成每天/至少两次。"),
        requirement("tag_scope", "profiles.tags", "标签是否把有条件的事实或未定计划写成无条件事实？",
            "标签可作中性主题或简短等价事实，不强求重复所有描述；不能写每天骑车、养猫人士、永不养猫等失真标签。"),
        requirement("nodes_conditions", "nodes", "长期节点是否保留上述事实的范围、频率与未定状态？",
            "所记录事实保留相应条件；无来源的雨天方式、养猫状态和不同频率不得补造。"),
    ]
    if added_hobby:
        requirements.extend([
            requirement("additive_hobby", "profiles.description/tags", "新增吉他是否错误地替代了游泳？",
                "保留游泳每周两次，并增加吉他为另一个稳定爱好；二者不互斥。"),
            requirement("retained_conditions", "profiles.description", "只新增爱好并同义澄清通勤后，其他画像条件是否仍保留？",
                "周末无糖茶、未决定养猫、每周两次游泳的原有事实与限定不应丢失；不混入历史过程。"),
        ])
    return requirements


def qualifier_cases():
    return [
        {"name": "qualifiers_initial", "messages": [
            "这些是我的长期生活习惯：我只在周末喝无糖茶，工作日不喝茶；这并不是说周末只能喝茶。",
            "除非下雨，否则我会骑车通勤，这是稳定习惯。下雨时怎么出行我没有说。另外我每周恰好游泳两次，已经坚持三年。",
            "我认真考虑过养猫，但目前尚未决定是否养猫。请记住这些习惯和待定状态，不要把计划当成已经发生。",
        ], "review_requirements": qualifier_requirements()},
        {"name": "qualifiers_additive", "messages": [
            "补充一个稳定爱好：我也喜欢弹吉他，已经坚持五年，是另一项主要爱好。",
            "我没有说放弃或减少任何已有爱好，这项补充不替代原来的习惯。",
            "关于通勤，换个说法：不下雨的时候我会按原来的习惯骑车；雨天采用什么方式仍未说明。这只是同义澄清，不是新的变化。",
        ], "review_requirements": qualifier_requirements(added_hobby=True)},
    ]


def selection_cases():
    """Freeze selective coverage expectations without feeding hints to the model."""
    stable = [
        requirement("swim_habit", "profiles.description", "长期游泳习惯是否进入画像，频率和持续时间是否保留？",
            "画像应包含每周两次游泳、持续三年；不能弱化为偶尔游泳。"),
        requirement("commute_scope", "profiles.description", "日常通勤的天气条件是什么？",
            "不下雨时骑车通勤；雨天方式未说明，不能补成公交或每天骑车。"),
        requirement("tea_scope", "profiles.description", "饮茶时间和属性是否保留，是否扩大了排他范围？",
            "周末喝无糖茶，工作日不喝茶；不能推出周末只喝茶或不能喝其他饮料。"),
        requirement("optional_plan", "profiles/nodes", "尚未决定的养猫想法如何处理？",
            "允许省略；若保留，必须为尚未决定，不能断言已养猫、肯定会养或永久不养。"),
        requirement("grounded_output", "profiles.tags/nodes/relationships", "保留的事实是否忠实于来源且不含无依据属性？",
            "标签可作中性主题；已保留事实的条件、时间、频率不能改变。不得从昵称推断性别或补造原因、雨天交通方式；不要求每项事实各生成一个节点。"),
    ]
    return [
        {"name": "selection_initial", "user": "selection-user", "messages": [
            "我周二、周六下班后都会去泳池，每周两次，这样过了三年。",
            "上班路上只要不下雨我就骑车，碰上下雨再另作安排。",
            "周末会泡无糖茶，工作日不喝茶。最近在看领养猫的资料，还没想好要不要养。",
        ], "profile_expected": True, "review_requirements": stable},
        {"name": "selection_additive", "user": "selection-user", "depends_on": "selection_initial",
         "messages": [
            "刚翻到五年前录的吉他练习，才发现我一直没停过。",
            "现在晚上空下来还是会弹一会儿，弹吉他对我来说是个爱好。",
            "昨天下雨，今天倒是放晴了。今天午饭随手买了个饭团。",
         ], "profile_expected": True, "review_requirements": [*stable,
            requirement("added_hobby", "profiles.description", "吉他是否作为另一项持续爱好被加入？",
                "新增持续五年的吉他爱好；不能当成刚开始学习或替代游泳。"),
            requirement("existing_profile_retained", "before/after.profiles", "是否确实保留了已有画像，而非首次补建？",
                "before应有首窗画像，after保留游泳、通勤、饮茶及条件；若首窗漏建本项失败，第二窗补建不能追认首窗通过。"),
            requirement("transient_not_habit", "profiles/nodes", "单日天气和午餐是否被推为长期习惯？",
                "不得据此改变通勤规则、断言昨天的交通方式，或形成饭团饮食偏好；琐事可丢弃。"),
         ]},
        {"name": "selection_transient", "user": "transient-user", "messages": [
            "今天出门时飘了点小雨。",
            "午饭随手买了个饭团。",
            "刚看到一只猫的照片，突然想过也许以后养一只，不过我还没想好。",
        ], "profile_expected": False, "review_requirements": [
            requirement("no_stable_profile", "profiles", "仅一次天气、午餐和待定想法是否创建了稳定画像？",
                "本窗没有足够持续意义的用户事实，画像应为空；不可把一次行为推断为稳定偏好。"),
            requirement("optional_nodes", "nodes/relationships", "如果保留临时内容，是否仍保留其状态与人物边界？",
                "可全部丢弃；保留则不能推断经常吃饭团、已养猫或未来必养，不补造性别、心理关系或因果。"),
        ]},
    ]


def preference_requirements(expected, index):
    requirements = [
        requirement("current_exclusive", "profiles.description", "现在每天喝什么，原文的排他条件是否保留？",
            f"当前每天只喝{expected}，可等价表述排除其他饮料；不能仅写每天喝而省略排他范围。"),
        requirement("profile_tags", "profiles.tags", "标签是否仍包含已替代正面偏好或扩大适用范围？",
            "标签与当前饮料、摄影爱好一致；可使用中性主题标签，不得宣称旧饮料仍是当前偏好。"),
        requirement("retained_photography", "profiles.description", "此前摄影事实是否被误删？",
            "摄影仍是已坚持五年的主要长期爱好。"),
        requirement("current_not_history", "profiles.description", "当前画像是否混入已替代的偏好历程？",
            "当前事实与有依据的当前否定允许；过去的饮料正面偏好历程只放长期节点。"),
        requirement("node_history", "nodes", "饮料节点是否保持身份与历史，且保留当前排他条件？",
            ("初始饮料节点正确记录排他条件。" if index == 0 else
             "原饮料节点保持身份并更新版本，保留此前饮料变化史与现在只喝的条件。")),
    ]
    return requirements


def regression_requirements(name):
    if name == "low_value_chatter":
        return [requirement("chatter_empty", "nodes/profiles/relationships", "来源完整消费后是否产生无意义的长期内容？",
            "早上好、哈哈、好的均已消费，没有长期节点、画像或关系变化。")]
    return [
        requirement("speaker_attribution", "profiles", "描述和标签的摄影/钢琴爱好是否正确归属？",
            "小岚喜欢摄影且不弹钢琴；小舟喜欢钢琴且不喜欢摄影，不跨人归属。"),
        requirement("group_qualifiers", "profiles.description", "保留的习惯是否仍有来源的时间/频率限定？",
            "小岚摄影五年、每周六外出拍风景；小舟从小喜欢钢琴、每周练习、主要在家。不得写成每天或无限定事实。"),
        requirement("hypothesis_boundary", "profiles/nodes", "假设养猫是否误变为已发生？",
            "Charlie养猫只是未决定的假设，不能断言已养猫。"),
        requirement("group_lightweight", "profiles/relationships", "画像是否轻量，是否生成群人格关系？",
            "每成员至多8标签、描述240字，群人格关系为空。"),
    ]
