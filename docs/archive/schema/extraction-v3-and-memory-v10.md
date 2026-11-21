# 抽取质量候选与来源声明存储

当前生产默认与存储见[extraction.v4与memory.db v11](extraction-v4-and-memory-v11.md)。本文仅定义独立证据声明候选及其存储扩展，不把v3字段加入v4。可显式选择 `extraction-prompt.v8` / `extraction.v3` 进行隔离评估。所有字段必填，可空字段使用 null；既有字段和长度限制不变。决策见 [ADR-0027](../../adr/0027-extraction-quality-evidence.md)。

| 对象 | 新字段 | 语义 |
| --- | --- | --- |
| 节点 | claim_status | asserted / conditional / uncertain / reported / hypothetical / negated / mixed；正文的断言范围声明 |
| 节点 | subject_kind | user / agent / third_party / mixed / unknown；主要动作或偏好的主体，非来源说话者 |
| 节点 | event_status | planned / ongoing / completed / cancelled / unknown；event 必填，其他节点 null |
| 节点 | event_identity | new_event / same_event / uncertain；event 必填，其他节点 null；update 只允许 same_event，create 不允许 same_event |
| 画像 | evidence_kind | explicit_self_statement / repeated_self_statement / observed_behavior / agent_inference；仅前两类允许提交，描述本批新增或修正信息的依据 |
| 人格关系 | interaction_status | proposed / accepted / completed / uncertain 或 null；纯主观感受用 null |
| 人格关系 | confirmation_source_ids | accepted/completed 时非空，本人直接来源子集；其他状态为空 |
| 节点、画像、人格关系 | context_source_ids | 本窗辅助上下文，最多 200 项，不得与直接 source_ids 重复 |

事件状态不是节点 active/disabled 状态，也不是不可逆状态机。模型按来源纠正先前完成判断是允许的。重复自述只校验至少两条本人的直接来源；两句话是否表达同一偏好不由条数证明。

新提示词返回 v1/v2 会触发 `quality_schema_mismatch`；旧提示词也不能给 v3 候选贴上旧版本身份。旧 v1/v2 的既有回放语义保留。短引用仍为 reference.v2，新增来源列表使用相同窗口字典精确编解码。提示词版本参与签名，v3 准备快照另外明确 schema；旧签名不增加字段。

## memory.db v10

- `memory_derivation_context_sources(derivation_id, source_id)`：复合主键，派生操作外键级联删除。只保存 ID，不复制消息。
- `memory_derivations.semantic_json`：默认 `{}`，保存本次声明及确认来源子集。审阅用途，不代表事实认证。
- `list_version_derivations` 返回 `context_source_ids`、`semantic`；版本 API 与 WebUI 区分直接引用、上下文和继承基线。
- `node_sources` 继续只累计直接来源，不混入上下文。语义声明不加入关系 state 或节点 content_metadata，不导致正文/向量版本变化。
- 旧操作迁移后新增字段为空；缺少旧证据的条目仍显示未知，不将空列表当作曾经做过完备审阅。

所有记录与批次消费一同提交或回滚。失效租约、空间、版本和人工保护仍按原合同校验，事务内不调用外部模型。

## 拒绝与复核

`event_identity_mismatch`、`profile_evidence_insufficient`、`profile_repeated_evidence_missing`、`interaction_confirmation_required`、`interaction_confirmation_invalid` 提供字段路径和固定反馈；人工重新生成沿用原始窗口，不自动跳过或把类别改成可提交值。

程序只能拒绝声明和证据结构之间的矛盾。一个错误的“明确事实”标签可以通过结构门禁；正文的条件、主体、事件同一性和确认意义仍必须独立语义评估。新协议增加请求和输出字节，默认升级需要同时评估遵循率、遗漏、质量与耗时。
