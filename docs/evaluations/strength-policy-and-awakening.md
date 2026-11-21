# 强度策略、主动唤醒与设置验证

## 参考评估

先检查现有实现与文献相关项目，评估可复用边界，再实现符合本插件契约的纯计算与事务逻辑。以下资料仅用于设计参考，未复制其实现，也未声称验证过外部服务的效果。

| 资料 | 可核对实现或说明 | 对本插件的适用性 |
| --- | --- | --- |
| [MemoryOS 中期热度](https://github.com/BAI-LAB/MemoryOS/blob/587ed7755c7aed179965792830ff1b5ad9a6fa92/memoryos-pypi/mid_term.py#L20)；仓库 Apache-2.0 | `alpha*N_visit + beta*L_interaction + gamma*R_recency`，recency 使用时间指数损失；粒度为会话热度 | 可参考使用访问和近期性维持活跃度，但没有节点重要性减速、有限普通强化及门限唤醒契约，不能直接替代 |
| [mem0 Memory Decay 文档](https://github.com/mem0ai/mem0/blob/26cdefb46600c0fa5953c4b5ee5003c125539fc4/docs/platform/features/memory-decay.mdx)；仓库 Apache-2.0 | 托管平台功能说明：排序倍率 0.3～1.5、只触碰最终返回结果、最近 20 次触碰 | 可参考有界增益与只强化交付结果；该文档描述软排序偏置，不做门限过滤，且不构成已核实的开源后端实现 |

NeoMemory 的唤醒是恢复被动资格，普通强化是有额度的累积增益。这两个职责用既有基准字段和零增益账本表达，避免为唤醒引入另一套永久状态或长期扫描任务。正式规则见 [ADR-0030](../adr/0030-strength-decay-reinforcement-and-awakening.md)。

## 可配置参数

全部位于 WebUI「设置 → 高级」，保存立即作用于新工作。AstrBot 基础配置继续只呈现基础开关。

| 字段 | 默认 | 可调范围 |
| --- | ---: | ---: |
| `strength_time_scale_seconds` | 604800 | 1～31536000 秒 |
| `strength_time_rate` | 0.03 | 0～1 |
| `strength_max_time_loss` | 0.15 | 0～0.15 |
| `strength_activity_loss_per_round` | 0.001 | 0～1 |
| `strength_max_activity_loss` | 0.6 | 0～0.6 |
| `strength_importance_decay_weight` | 1 | 0～10 |
| `strength_wake_margin` | 0.01 | 0～0.1 |
| `reinforcement_gain` | 0.02 | 0～0.2 |
| `reinforcement_neighbor_multiplier` | 0.25 | 0～1 |
| `reinforcement_active_multiplier` | 2 | 1～10 |
| `reinforcement_diminishing_power` | 1 | 0.1～4 |
| `reinforcement_cooldown_seconds` | 300 | 0～86400 秒 |
| `reinforcement_max_per_turn` | 0.12 | 0～0.12 |
| `reinforcement_max_total` | 0.2 | 0～0.2 |

原有 `recall_min_strength` 仍是被动资格门限，默认 0.1。减速系数为 0 关闭重要性对速率的调节，不关闭衰减；时间速率、活动速率或普通增益分别设为 0，可关闭对应作用。普通增益或额度设为 0 不关闭主动唤醒。只有本空间成功抽取推进活动轮次。

## 工程验证

验证使用临时数据库、合成节点和可控向量，不使用私人固定会话调优，不调用真实 LLM 或 Embedding API。

- [纯计算测试](../../tests/test_strength.py)：高重要性减慢时间/活动损失且不改变上限；强度基准与内容重要性独立；普通增益随强度上升递减；直接/邻居、主动/被动倍率保持约束。
- [召回存储测试](../../tests/test_recall_store.py)：单轮去重、正增益冷却、跨调用额度、累计上限、内容更新重置、范围/状态/版本校验及并发事务。
- [唤醒与召回集成](../../tests/test_recall_strength.py)：强化额度已满与为零时都可独立唤醒；不刷新已耗额度；历史事实不复活；只处理实际返回条目；下一次被动查询恢复资格；零余量、门限 1、降额、失败回滚及跨连接重复唤醒。
- [配置与运行时](../../tests/test_recall_runtime.py)：保存配置不重写节点或重新打开存储；新工作使用新权重，保留的旧请求使用原策略；[设置测试](../../tests/test_settings.py)验证高级分组、逐项恢复和上限校验。
- [真实宿主路由浏览器检查](../../tests/ui/host_smoke.py)：Chromium 通过 AstrBot 实际配置 API 修改重要性系数和唤醒余量，保存后运行时立即应用；单项重置保留另一项，插件实例及节点内容不变，重新加载后配置仍存在。使用临时宿主数据与模拟 Provider，不等于实机部署验收。

## 效果边界

这些验证证明规则、预算和持久化行为，不证明真实长周期聊天的最优参数。默认值尚需独立多空间、不同活跃频率的数据评估；不能依据同一私人会话针对性调整。高重要性降低速率但不获得永久免衰减；节点有效也仍须满足相似度、候选数量、正文版本、预算等召回条件。

主动唤醒可能使下一次强度高于门限，但不保证一定注入。余量 0 或门限 1 只保证重置瞬间资格；现有强化大于目标时，基准下限 0 会产生略高于目标的恢复值。热改衰减参数会重新计算上次基准重置以来的损失，已存在强化不回放新权重；这些行为无需迁移或清空数据库。
