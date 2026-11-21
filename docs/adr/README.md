# NeoMemory Architecture Decision Records

本目录保存独立架构决策与提案。当前状态见[能力状态](../阶段实施计划.md)，有效要求见[开发计划](../开发计划.md)，工作顺序见[后续开发路线](../后续开发路线.md)。依据明确用户要求与有效决定判断冲突，不能仅因代码如此就改写契约。

## 使用规则

- `Accepted` 表示有效设计约束，不表示实现与质量验收已经完成；具体缺口列在能力状态页。
- `Proposed` 表示技术方案待收敛或验证；其中明确引用的用户需求仍有效，不能因技术方案待定而取消需求。
- `Deprecated` 表示已被后续 ADR 替代，但保留用于理解历史。
- 已接受的决策原则上不直接改写历史内容。发生变化时新增 ADR，并在旧 ADR 中注明替代者。
- 参数默认值属于可配置策略。除非 ADR 明确要求兼容性，否则调优参数不需要新 ADR；改变数据语义、作用域、可靠性边界或任务一致性时需要新 ADR。

## 决策索引

| 编号 | 决策 | 状态 |
| --- | --- | --- |
| [ADR-0001](0001-identity-and-memory-scopes.md) | 使用人格、逻辑用户和场景作用域隔离记忆 | Accepted |
| [ADR-0002](0002-source-derived-cache-storage.md) | 原始库、派生库和可清空缓存分离 | Accepted |
| [ADR-0003](0003-memory-node-model.md) | 采用可独立理解的记忆节点和有限关系 | Accepted |
| [ADR-0004](0004-extraction-and-llm-orchestration.md) | 选择性抽取、受约束提示词和分级 LLM 调用 | Accepted |
| [ADR-0005](0005-lightweight-retrieval-and-injection.md) | 被动召回使用轻量向量流程，注入采用预生成记忆文本 | Accepted |
| [ADR-0006](0006-memory-strength-decay-and-reinforcement.md) | 重要性、时间衰减、活动衰减和有限强化共同决定强度 | Accepted |
| [ADR-0007](0007-jobs-consistency-and-rebuild.md) | 后台任务使用作用域锁、检查点和 generation 切换 | Accepted |
| [ADR-0008](0008-human-governance-and-data-lifecycle.md) | 派生记忆允许人工治理，原始事实与派生数据分离处理 | Accepted |
| [ADR-0009](0009-low-cost-group-observation.md) | 群聊旁听采用低资源、低结构复杂度策略 | Accepted |
| [ADR-0010](0010-webui-and-provider-integration.md) | 使用五栏 WebUI 和 AstrBot Provider 作为模型入口 | Accepted |
| [ADR-0011](0011-prompt-contracts-and-evaluation.md) | 提示词、JSON schema 和真实模型回放独立版本化 | Accepted |
| [ADR-0012](0012-extraction-commit-and-protection.md) | 独立人工保护、抽取事务和幂等完成凭据 | Accepted |
| [ADR-0013](0013-versioned-memory-coverage.md) | 覆盖证据绑定事件与原子两端正文版本 | Accepted |
| [ADR-0014](0014-lightweight-recall-runtime.md) | 有界缓存索引、临时注入和宿主工具作用域 | Accepted |
| [ADR-0015](0015-durable-maintenance-and-replay.md) | 持久任务租约、治理事务及独立代重构；退休代保留由ADR-0051替代 | Partially superseded |
| [ADR-0051](0051-rebuild-discards-retired-content.md) | 成功重构删除旧代内容与历史，新记录独立计数 | Accepted |
| [ADR-0016](0016-durable-ingress-and-context-completion.md) | 持久待写、默认人格跳过与连续性上下文 | Accepted |
| [ADR-0017](0017-management-console-and-config-lifecycle.md) | 五栏管理界面与笔记任务；配置生命周期由 ADR-0022 替代 | Partially superseded |
| [ADR-0018](0018-conservative-coverage-selection.md) | 保守覆盖选择及预算取舍，生产默认尚未修改 | Proposed |
| [ADR-0019](0019-preserve-extraction-input.md) | 宿主逐调用保留抽取输入的接口及兼容边界 | Proposed |
| [ADR-0020](0020-profile-exact-edit-candidate.md) | 保持轻量存储的画像精确片段更新候选 | Proposed |
| [ADR-0021](0021-companion-external-memory-provider.md) | 接入陪伴插件外部记忆 Provider | Accepted（宿主联调待完成） |
| [ADR-0022](0022-live-settings.md) | 宿主基础开关、WebUI 完整配置与运行时应用 | Accepted |
| [ADR-0023](0023-window-references-and-regeneration.md) | 窗口短引用、严格回译与失败候选重新生成；正文引用范围由 ADR-0024 扩展 | Partially superseded |
| [ADR-0024](0024-persona-voice-and-user-references.md) | 人格第一人称、自由主观关系与逻辑用户昵称渲染；关系主观判断范围由ADR-0034明确 | Partially superseded |
| [ADR-0025](0025-shared-context-budget.md) | 共享UTF-8字节预算、完整条目选择与可配置注入优先级；默认值见ADR-0047 | Partially superseded |
| [ADR-0026](0026-version-provenance.md) | 正文版本、直接引文与更新基线分别记录 | Accepted |
| [ADR-0027](0027-extraction-quality-evidence.md) | 独立证据声明候选与来源存储 | Accepted（候选未作默认） |
| [ADR-0028](0028-normal-memory-narrative.md) | 正常叙述、身份引用与约定称呼 | Accepted |
| [ADR-0029](0029-memory-titles-and-inline-inspection.md) | 标题与条目内版本/来源查阅 | Accepted（当前实机版本待验收） |
| [ADR-0030](0030-strength-decay-reinforcement-and-awakening.md) | 重要性减速、递减强化与独立主动唤醒 | Accepted |
| [ADR-0031](0031-node-emotional-context.md) | 节点经历情绪、内容版本与完整预算注入 | Accepted |
| [ADR-0032](0032-readable-length-contract.md) | 可读长度、独立编码容量与版本兼容 | Accepted |
| [ADR-0033](0033-context-order-and-observability.md) | 辅助资料可选顺序与装配诊断；默认预算见ADR-0047 | Partially superseded |
| [ADR-0034](0034-relationship-affect-autonomy.md) | 人格关系的心理职责、自主判断与完整状态承接 | Accepted |

| [ADR-0035](0035-format-recovery-and-task-deletion.md) | 独立结构化请求与后台任务删除；纠正范围和额度见ADR-0037 | 部分被替代 |
| [ADR-0037](0037-bounded-candidate-recovery.md) | 默认3次有界候选纠正、持久预算与后台索引期限 | Accepted |
| [ADR-0038](0038-profile-duties-and-practical-quality.md) | 画像独立职责、按实际收益与错误影响验收、少量瑕疵可试用 | Accepted |
| [ADR-0039](0039-query-balanced-extraction-context.md) | 后台抽取逐查询候选覆盖，保持总量与近期份额 | Accepted |
| [ADR-0040](0040-context-budget-4096.md) | 注入预算决策留档；当前默认见ADR-0047 | Superseded |

## 使用时必须保持的边界

- 原始记录是重构的事实输入，记忆节点是可替换的派生结果。
- 作用域、来源引用、任务状态和提交由程序控制，LLM 只产生经过 schema 校验的候选变更。
- 可清空缓存不能承载记忆正确性；失败恢复所需的检查点必须持久化。
- 被动召回默认不增加额外生成模型调用；复杂语义综合应显式进入主动工具或后续 ADR。

存储分工以ADR-0002结合ADR-0016理解，包含持久待写库；配置使用ADR-0022，叙述使用ADR-0028，人格关系心理职责与自主判断使用ADR-0034，标题使用ADR-0029，节点情绪使用ADR-0031，预算使用ADR-0025；当前生产默认和显式历史映射见[提示词协议](../提示词与输出协议.md)。旧记录的执行阶段和测试描述不作为当前进度。

后续细则省略原要求不表示已批准撤销。未完成的语义质量与功能验收继续按当前能力状态处理；独立主动唤醒使用ADR-0030，不能与普通强化上限混同。

- [0036：人格自己的长期记忆表达](0036-persona-authored-memory-prose.md)：以人格语气组织事实与已有感受，保持单份正文及来源、状态边界。

- [ADR-0041：跨经历主题的可恢复归纳](0041-topic-consolidation.md)：保留事件粒度，完整成员及版本依据、低频任务和预算内主题入口。

- [ADR-0042：显式旧节点依据与主题成员版本一致性](0042-explicit-version-basis.md)：实际引用与组织关联分开记录，两条主题写入路径使用一致的成员版本依据。

- [ADR-0043：抽取字段标准与schema派生的有界纠正反馈](0043-schema-aligned-recovery.md)。

- [ADR-0044：主题与短期摘要共用标准反馈、固定输入和持久纠正额度](0044-shared-topic-summary-recovery.md)。

- [ADR-0045：纠正反馈绑定固定请求协议](0045-fixed-schema-recovery.md)：目标schema、字段引用域与条件限额一致，旧反馈保持显式兼容。

- [ADR-0046：已定义检查标准与纠正指标保持一致](0046-standard-metrics-feedback.md)：首次约束、条件与动态指标、全项覆盖矩阵及运行冲突边界。

- [ADR-0047：默认记忆注入预算15000字节](0047-context-budget-15000.md)：约5000个常用汉字容量，保留字节计量与完整条目装配。

- [ADR-0048 原始观察时间与主题成员结构](0048-source-observation-topic-structure.md)
- [ADR-0049 人格关系的自然内心表达](0049-relationship-inner-voice.md)

- [ADR-0050 节点内容更新时间使用原始观察时间快照](0050-content-observation-clock.md)

- [ADR-0052 逻辑用户命名与账号归属管理](0052-logical-user-account-management.md)

- [ADR-0053 人格关系的长期理解与近期余韵](0053-subjective-understanding-and-aftereffect.md)
- [ADR-0054 每个节点情绪标签独立强度](0054-independent-emotion-intensities.md)
- [ADR-0055 人格关系整合心理意义并限制生成篇幅](0055-compact-relationship-consolidation.md)：新默认500/150可读上限；替代ADR-0053的新生成长度上限。
- [ADR-0056 抽取优先使用 JSON Schema](0056-schema-output-fallback.md)
- [ADR-0057 按当前记忆重构主题](0057-topic-only-rebuild.md)
- [ADR-0058 按板块重构与版本保留](0058-selective-rebuild-and-history.md)
- [ADR-0059 私聊长期记忆的图片归属与重构依据](0059-private-image-evidence.md)
- [ADR-0060 首次公开发布的版本基线](0060-first-public-release.md)
