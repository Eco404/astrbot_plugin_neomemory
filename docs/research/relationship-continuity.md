# 人格关系滚动承接研究

系统把完整上一版关系交给抽取模型，并以完整新正文替换。持续承接要求提示词明确保留仍成立的关系基调；历史版本保存不能自动补回下一轮已经遗漏的内心状态。承接还需要同时允许真实变化和自由主观表达，不能变成永久冻结旧感受。

## 已核验参考

| 来源与版本 | 许可证及维护证据 | 借鉴范围与限制 |
| --- | --- | --- |
| [MemoryOS](https://github.com/BAI-LAB/MemoryOS/tree/587ed7755c7aed179965792830ff1b5ad9a6fa92)，`587ed7755c7aed179965792830ff1b5ad9a6fa92` | Apache-2.0；本地快照提交2026-07-07 | [画像提示词](https://github.com/BAI-LAB/MemoryOS/blob/587ed7755c7aed179965792830ff1b5ad9a6fa92/memoryos-pypi/prompts.py#L153)明确保留已有认识并融合新信息；[执行路径](https://github.com/BAI-LAB/MemoryOS/blob/587ed7755c7aed179965792830ff1b5ad9a6fa92/memoryos-pypi/memoryos.py#L154)一次调用生成完整画像再替换。可借鉴滚动融合，不能移植90维画像和推断规则约束人格关系。 |
| [Graphiti](https://github.com/getzep/graphiti/tree/5c27edfa8d3e0f49b2571bcebb4261bcc89ee037)，`5c27edfa8d3e0f49b2571bcebb4261bcc89ee037` | Apache-2.0；本地快照提交2026-10-25 | [实体摘要规则](https://github.com/getzep/graphiti/blob/5c27edfa8d3e0f49b2571bcebb4261bcc89ee037/graphiti_core/prompts/extract_nodes.py#L546)结合已有持久内容与新来源，无新持久信息时保持原文。可借鉴保留、融合和不更新的区别；第三人称事实摘要规则不适用于人格主观联想。 |
| [Hermes](https://github.com/NousResearch/hermes-agent/tree/d05095663342556788cf2a198bfa7844cc7968ab)，`d05095663342556788cf2a198bfa7844cc7968ab` | MIT；本地快照提交2026-10-25 | [文档](https://github.com/NousResearch/hermes-agent/blob/d05095663342556788cf2a198bfa7844cc7968ab/website/docs/user-guide/features/memory.md#L124)记录模型把整条替换误当局部替换，丢失其他内容；[span patch实现](https://github.com/NousResearch/hermes-agent/blob/d05095663342556788cf2a198bfa7844cc7968ab/tools/memory_tool_store.py#L431)保持未修改片段。仅作备选，当前不增加匹配、schema和重试，也不限制自由自然语言重组。 |

以上是已核验快照，不代表上游最新版本。只借鉴公开设计，没有复制第三方实现。MemoryOS的README提供论文[Memory OS of AI Agent](https://arxiv.org/abs/2506.06326)；本项未阅读全文，不依赖其指标宣称效果。

Graphiti的[事实去重与失效规则](https://github.com/getzep/graphiti/blob/5c27edfa8d3e0f49b2571bcebb4261bcc89ee037/graphiti_core/prompts/dedupe_edges.py#L49)还明确：只有新事实使对应旧断言不能再同时成立，才判断替代；不能仅凭更近时间、相同人物的新事件或部分职责进展推翻其他事实。其不同日期的两次活动示例适合关系中的客观约定范围判定，但不限制人格主动重新理解主观感受。

## 候选边界

`extraction-prompt.v12`使用现有`extraction.v4`，在v11上仅补充关系承接规则：完整融合仍成立的旧状态与有意义新变化；换话题不等于旧感受失效；明确纠正的事实与取消约定必须调整；没有实质变化时关系为null。继承状态由上一版本支持，本窗来源只支持本次变化。人格保持自由第一人称，不要求情绪方向、等级或固定段落。

候选不增加模型调用、存储结构或独立累积窗口；显式旧v11保持字节稳定。生产选择以独立评估为依据，不能仅因协议测试通过就宣称连续性改善。

`extraction-prompt.v13`在v12上加入事实与约定的对象、时间范围：过去已取消与未来未定可以并存，另一次邀请不能反转旧事件状态；必要时明确指代对象。它同样保持v4 schema，不要求固定关系结构，也不限制主观感受变化。v12仍保留独立模板，避免评估身份发生漂移。

## 独立评估

使用`scripts/probe_relationship_continuity.py`先冻结两条各三窗口的合成链，对照v11/v12，共最多12次HTTP。两并发、链内顺序；每一分支下一窗口读取该分支真实提交的上一状态，不能每窗重新提供理想答案。种子关系为给定测试前提，不计作模型生成成绩。

第一链覆盖稳定信任、未报名未学习的工作坊计划、有意义的倾听互动、无关摆灯话题。第二链覆盖散步取消、独立修理经历、澄清仍愿交流但未接受未来线下邀请。初始关系、窗口输入和审阅问题均在调用前冻结；不读取私人固定会话。

逐窗人工审阅旧基调保留、真正变化、错误事实是否移除、第一人称自由表达、无实质变化时是否避免改写，以及计划/完成标题的忠实性。等价改写可以通过，不用关键词匹配代替语义判断。结构、来源、身份、版本、HTTP次数、token及耗时另行记录。输出保存在被忽略的`data/evaluations/`，报告保留失败与未验证范围。

若融合规则仍反复遗失重要内容，或关系变化确实依赖尚未进入旧状态的跨窗口线索，再比较有限互动缓冲、补取既有证据和局部修改。当前不把扩大窗口作为未经验证的结论。

固定旧状态的双模型对照使用`scripts/probe_relationship_controls.py`，将模型与提示词的效果和前序状态差异分开。它还支持对已冻结的合成请求做相同user输入的诊断回放；此模式只执行schema检查，报告明确标记`schema_only`，不与完整事务回放混为一谈。实际结果见[独立评估](../evaluations/relationship-continuity-20261113.md)。
