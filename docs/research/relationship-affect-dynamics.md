# 人格关系的心理承接与近期背景研究

人格关系承载人格对这段关系的感受、看法、期待、矛盾和相处态度。长期记忆负责具体事件和事实；关系正文可保留解释心理变化所需的最小缘由，不应持续继承日程、任务和事件清单。人格保留自由的第一人称表达，不要求情绪标签、固定段落、积极走向或关系等级。

本文保存v20基线下的独立探索分支与冻结判据，不作为当前默认关系验收标准；当前采用心理职责与自主判断，见[ADR-0034](../adr/0034-relationship-affect-autonomy.md)与[自主表达验证](../evaluations/relationship-autonomy-20261116.md)。动态窗口与衰减未采用。实验入口为 `scripts/probe_relationship_dynamics.py`；参数和用例在真实调用前冻结。私人固定会话及其回放结果不参与候选设计或调参。

## 参考核验

以下资料仅提供设计参考，不是本插件情感连续性效果的证明。除 Generative Agents 为公网固定提交核查外，其余为 `data/references/` 中的本地快照；快照日期不能作为上游最新维护状态。

| 来源 | 版本、许可与维护证据 | 可借鉴设计与适用限制 |
| --- | --- | --- |
| [MemoryOS](https://github.com/BAI-LAB/MemoryOS/tree/587ed7755c7aed179965792830ff1b5ad9a6fa92) | `587ed7755c7aed179965792830ff1b5ad9a6fa92`；Apache-2.0；快照提交日期2026-07-07 | [画像提示词](https://github.com/BAI-LAB/MemoryOS/blob/587ed7755c7aed179965792830ff1b5ad9a6fa92/memoryos-pypi/prompts.py#L153)融合旧认识与新观察；它面向90维用户画像，不是人格内心，不能移植其固定维度。 |
| [Graphiti](https://github.com/getzep/graphiti/tree/5c27edfa8d3e0f49b2571bcebb4261bcc89ee037) | `5c27edfa8d3e0f49b2571bcebb4261bcc89ee037`；Apache-2.0；快照提交日期2026-10-25 | [失效判定](https://github.com/getzep/graphiti/blob/5c27edfa8d3e0f49b2571bcebb4261bcc89ee037/graphiti_core/prompts/dedupe_edges.py#L85)明确“较新”本身不构成旧状态失效。[实体摘要](https://github.com/getzep/graphiti/blob/5c27edfa8d3e0f49b2571bcebb4261bcc89ee037/graphiti_core/prompts/extract_nodes.py#L546)要求尽量保留事实，直接用于人格关系会加剧事实堆积；其禁止主观推断的要求也不能照搬到人格自己的感受。 |
| [Hermes](https://github.com/NousResearch/hermes-agent/tree/d05095663342556788cf2a198bfa7844cc7968ab) | `d05095663342556788cf2a198bfa7844cc7968ab`；MIT；快照提交日期2026-10-25 | [有界记忆](https://github.com/NousResearch/hermes-agent/blob/d05095663342556788cf2a198bfa7844cc7968ab/website/docs/user-guide/features/memory.md#L9)强调筛选；[局部patch](https://github.com/NousResearch/hermes-agent/blob/d05095663342556788cf2a198bfa7844cc7968ab/tools/memory_tool_store.py#L429)保持无关内容。[整体替换导致信息丢失的说明](https://github.com/NousResearch/hermes-agent/blob/d05095663342556788cf2a198bfa7844cc7968ab/website/docs/user-guide/features/memory.md#L124)是风险参考，不意味着自由自然语言关系应改成字面patch；后者也可能固化旧情绪和冗余。 |
| [Generative Agents](https://github.com/joonspk-research/generative_agents/tree/fe05a71d3e4ed7d10bf68aa4eda6dd995ec070f4) | `fe05a71d3e4ed7d10bf68aa4eda6dd995ec070f4`；Apache-2.0；固定提交日期2023-08-11，研究原型 | [反思与证据关联](https://github.com/joonspk-research/generative_agents/blob/fe05a71d3e4ed7d10bf68aa4eda6dd995ec070f4/reverie/backend_server/persona/cognitive_modules/reflect.py#L38)、[独立thought生成](https://github.com/joonspk-research/generative_agents/blob/fe05a71d3e4ed7d10bf68aa4eda6dd995ec070f4/reverie/backend_server/persona/cognitive_modules/reflect.py#L99)、[累计重要性触发](https://github.com/joonspk-research/generative_agents/blob/fe05a71d3e4ed7d10bf68aa4eda6dd995ec070f4/reverie/backend_server/persona/cognitive_modules/reflect.py#L135)支持事件与心理意义分开、反思保留来源。其多次生成、评分和向量调用不适合直接复制到本插件抽取流程。 |

### 是否真正启用了衰减

MemoryOS 的 [compute_time_decay](https://github.com/BAI-LAB/MemoryOS/blob/587ed7755c7aed179965792830ff1b5ad9a6fa92/memoryos-pypi/utils.py#L228)实现自然时间指数衰减；[compute_segment_heat](https://github.com/BAI-LAB/MemoryOS/blob/587ed7755c7aed179965792830ff1b5ad9a6fa92/memoryos-pypi/mid_term.py#L26)将它与访问次数、互动长度相加。但是[实际检索评分](https://github.com/BAI-LAB/MemoryOS/blob/587ed7755c7aed179965792830ff1b5ad9a6fa92/memoryos-pypi/mid_term.py#L326)中的时间衰减被注释。可确认中期heat使用了近期性，不能声称该快照的检索或人格情感已通过衰减平滑。

Generative Agents 的 [extract_recency](https://github.com/joonspk-research/generative_agents/blob/fe05a71d3e4ed7d10bf68aa4eda6dd995ec070f4/reverie/backend_server/persona/cognitive_modules/retrieve.py#L132)按照节点序号取指数值；调用处[按last_accessed升序排列](https://github.com/joonspk-research/generative_agents/blob/fe05a71d3e4ed7d10bf68aa4eda6dd995ec070f4/reverie/backend_server/persona/cognitive_modules/retrieve.py#L224)，并在召回后刷新访问时间。这个实现不能直接当作本任务中“新互动优先、旧感受不因重复总结而重获年龄”的可靠合同。这里只借鉴反思与来源分离，不复制其评分实现。

相关论文为 [Memory OS of AI Agent](https://arxiv.org/abs/2506.06326) 和 [Generative Agents: Interactive Simulacra of Human Behavior](https://arxiv.org/abs/2304.03442)。本项结论基于上述可定位实现，不引用论文指标证明本插件效果。

## 冻结探索分支

| 分支 | 内容 | 用于回答的问题 |
| --- | --- | --- |
| `baseline` | 冻结的v20提示词和上一版完整关系 | 对照承接机制的表现 |
| `affect` | 替换旧关系承接与关系事实范围规则，突出心理职责、简短缘由、旧状态可演变、无意义变化可返回null；保持其他抽取协议 | 仅改变内容职责是否已足以减少事实堆积并维持连续性 |
| `weighted` | 与 `affect` 相同，额外提供有限过去原始互动组及关注权重 | 额外近期背景及其选择是否产生值得付出成本的增益 |

`weighted` 原型使用：

```text
weight = 2 ^ (-后续互动组间隔 / 3)
         × max(0.65, 2 ^ (-来源时间相隔天数 / 30))
```

权重低于0.3的组不进入候选；按权重选取至多3组，消息正文累计至多1800字符，整组保留、不截断单组；选中组再按来源顺序呈现。当前来源始终提供，辅助组不成为本轮可引用的新增 `source_ids`。这些数值仅为冻结实验参数，不是产品默认。

该机制的准确边界是：

- 权重控制辅助上下文选取和关注，不对信任、失落等自然语言感受做数学插值，不代表事实真实性、情绪正负或关系价值。
- 每个来源互动组推进年龄，普通交流也推进；并非仅有意义的关系变化推进。它不能证明按关系变化次数衰减的方案有效。
- 原型没有模型或规则判定语义相关性，不能称为按心理冲突自动扩窗。
- 低权重不表示边界取消、过去未发生或冲突已和解；相同内容在旧关系与辅助组重复出现不能作为多份独立证据。
- 自然时间系数有下限，但条目间隔仍可使辅助组退出窗口；稳定基调的承接依赖关系语义，不能由该下限保证。
- `weighted` 与 `affect` 同时存在额外上下文和权重选择差异；若有收益，仍需新对照区分“增加历史材料”与“加权策略”各自的贡献。

## 冻结探索的验证方式与判据

实验使用独立合成的人格和多轮互动，链内将各分支真实提交的关系传入下一窗口；不逐窗回填理想答案。固定前提不算模型生成成绩。结构校验、来源校验、事务提交和自然语言语义分别记录。

逐窗审阅关注：

1. 当前心理意义是否占主导，事实是否只保留必要缘由，而非事件清单。
2. 最新有意义互动能否改变焦点，原有信任或边界是否无故丢失。
3. 普通话题是否被误当和解；明确修复是否能使旧歉意、失落退到背景。
4. 允许主观猜测、不确定性和矛盾，不能把猜测写成对用户动机的事实断言。
5. 没有关系变化时是否可以不改写；不能为满足情绪目标制造亲密、依赖、敌意或心理诊断。
6. 第一人称是否自然，是否被固定结构或持续积极化取代。
7. 长期事实抽取是否仍忠实，不能以漏掉事实换取关系精简。

事实句比例、正文长度、输入token和延迟只作诊断。空泛的情绪文本也能很短、很少事实，不能因此判为连续性良好。真实调用的结构通过不代表心理表达质量通过。

现有合成链覆盖稳定信任下的误伤、无关事实、有限澄清、明确修复、长期停用，以及降低联系频率、普通回复、边界重申、当下倾诉与长期距离并存。对更长周期、复杂误解、更多人格以及不同来源平台的结论仍需独立材料。

## 采用条件与持久化边界

若 `affect` 已能减少事实且维持承接，而 `weighted` 未在独立材料稳定改善，优先采用职责层改进，保留窗口与衰减为候选。不能因为实现了公式就认定连续性更好；也不能根据私人样本的具体用语调参数。

如后续采用近期背景或变化缓冲，需要明确来源时间、互动组边界、去重、重放顺序和持久化/可重建合同。生成、浏览、召回和重试不应刷新来源年龄；重构不能用执行时间代替来源时间。影响后续语义正确性的材料不能只依赖随时可清空的临时缓存，否则重启会改变关系演变。历史完整关系版本不应作为多份独立证据重复累加。

稳定基调与近期状态可以拥有不同更新速度，但不必在WebUI中变成固定情绪分栏。持久层结构、正式参数、配置界面与迁移均需在候选效果成立后另行设计；本原型不要求增加被动召回的模型调用。
