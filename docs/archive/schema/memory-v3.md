# memory.db v3：抽取完成凭据与强度基准

v3 在[基础 schema](memory-v1.md)和 v2 独立保护迁移上追加字段，不复制来源正文。

## 新字段

| 表 | 字段 | 含义 |
| --- | --- | --- |
| memory_generations | activity_round INTEGER | 当前 generation 成功抽取次数，初始 0 |
| memory_nodes | strength_reset_at REAL | 最近一次正文/重要性更新或有效主动唤醒重置的 Unix 秒；旧库以 updated_at 初始化 |
| memory_nodes | strength_reset_round INTEGER | 重置时本 generation 的轮次；新批次节点用该批次提交后的轮次 |
| memory_nodes | reinforcement REAL | 已合格召回产生的累计普通强化量；正文/重要性实际变化时清零，主动唤醒保留已用额度 |

memory_nodes.strength 保存最近重置时的基准强度；当前强度通过 core.strength.calculate_strength 按重要性减速、时间差、轮次差、强化量懒计算。不能把每次查询的结果回写基准，否则会重复扣减。正文或重要性实际变化时，基准设为新重要性并清空普通强化；同内容同重要性不因重复更新恢复强度，标题单改也不重置。

主动工具实际返回有效的低强度 active 节点时，可以独立重置基准、时间和轮次以恢复被动资格，保留重要性和已用普通强化总量。此时基准不必等于重要性，懒计算使用重要性控制速率；不改变正文版本、更新时间、历史状态或人工保护。完整规则见 [ADR-0030](../../adr/0030-strength-decay-reinforcement-and-awakening.md)，不增加数据库迁移。

## extraction_batches

每个已成功提交的窗口一条记录。它是正确性所需的完成凭据，不是缓存或完整任务系统。

- batch_id：空间、generation、有序来源 IDs 的摘要。
- space_id、generation_id：引用所属空间和代。
- activity_round：该批次提交后的轮次，generation 内唯一。
- prompt_version、schema_version、model_id：执行协议与模型标识，不含密钥。
- input_digest、output_digest：完整准备输入及已校验候选的摘要。
- result_json：changes/no_change 及本批 local_ref→node_id 映射；不保存重复节点正文。
- committed_at：提交时间。

## extraction_batch_sources

- batch_id：外键引用完成凭据。
- generation_id、source_id：该 generation 内来源只能消费一次，跨库存在性由注入回调验证。
- ordinal：批次内顺序，在 batch 内唯一。
- stream_id、sequence_no：范围顺序，用于禁止较早输入在较新批次后覆盖画像。

这两个来源顺序字段保留提交范围所需的最小快照，不复制原始正文。按 generation/stream/sequence 建索引。source.db 的 last_derived_sequence 不作为推进依据：正确性以与派生写入原子提交的凭据为准，避免跨数据库提交中断形成假进度。

## 事务与重试

1. 在 source.db 获取固定范围并验证身份；事务外构建提示词、调用模型和严格解析。
2. 在 memory.db 单一事务校验活动代、轮次、全部输入节点/画像/关系快照（包括 no_change）、来源未消费和顺序上限。
3. 写节点、版本、来源、关系、画像、人格关系与强度重置；再写完成凭据并推进轮次，一起提交。
4. 任一步失败全部回滚。重试原 PreparedExtraction 时比较摘要并返回相同凭据；换人格或上下文的同批次拒绝。已退休代不能继续应用。

后台任务据此查询完成范围，未完成来源仍在 source.db，可重新准备并调用模型；调度、失败队列和重构进度由独立持久任务系统管理，不能将任务状态写入可清空缓存。
