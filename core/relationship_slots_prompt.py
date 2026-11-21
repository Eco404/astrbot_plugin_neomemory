"""Persona relationship duties shared by extraction and component replay."""

COMPACT_RELATIONSHIP_RULES = """relationship_change只用于私聊；没有新的主观变化时为null。long_term是我对对方和这段相处较持久的理解、在意之处和看法；aftereffect是近期互动留在我心里的感受和余韵。两段都写我脑中自然展开的心里话，由persona和我的自主判断决定；不是用户画像、事实总结或相处说明书，不必重复写同一层意思。
known_relationship.state中的两段是此前心境，可以延续、调整或淡出，不是新互动或对方事实。允许偏爱、怀疑、误解、坚定看法和负面感受，不要求对方确认我的想法，不强迫升温；主体和实际行动仍按来源区分，不把我的理解变成对方已接受、承诺或完成的事实。
长期理解表达我现在如何看待对方和这段相处。发生有意义的变化时，结合旧理解重新组织完整正文；保留仍有影响的感受、矛盾及必要缘由，相同意思可以合并，失去心理意义的事实细节可以淡出。连续性不要求保留旧句或逐次补记经历；篇幅主要用于当下的心理理解，不逐件罗列互动。近期余韵只写当前仍有分量的感受，必要背景简短交代，不复述事情经过或重复长期理解。旧段超过当前上限也不为了缩短而宣称发生新的心理变化；无实质变化仍可keep，需要replace时输出符合当前上限的完整整合正文。
两段分别选择op：keep原样保留；replace填写该段完整的新正文；clear清空不再需要的该段。keep和clear的body必须为空字符串，replace必须非空。没有实质变化就keep，不为换话题改写，不因普通客套清空长期理解；两段均无需变化时整个relationship_change为null。事实背景只写理解心境所需的部分，不靠罗列会话或重复旧文填充。
source_ids只填写一次，引用本批触发这些变化的互动，至少一条本人的发言；程序依据引用的原始观察时间给replace段定观察锚点，keep和读取不刷新。观察时间不是正文所有事件的发生时间，不因重新生成把旧感受说成刚发生。expected_version新增为0，否则复制已有content_version。
long_term.body最多500可读字符，aftereffect.body最多150；两段上限独立，不设最低字数，不要求填满。完整合法{{user:ID}}计一个可读字符。关系不输出情绪分数、额外画像或事件字段。
"""

SLOT_BODY_RULE = """keep、replace、clear是op的操作值。op=replace时，body直接写这一段完整的内心想法，不能只填操作词；没有新的心境可写时用keep和空正文。"""
