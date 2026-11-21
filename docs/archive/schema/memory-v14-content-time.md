# 节点内容时间接口

当前数据库版本为 `memory.db v14`。规则见[ADR-0050](../../adr/0050-content-observation-clock.md)。

| 表与字段 | 语义 |
| --- | --- |
| `memory_nodes.content_updated_at REAL NULL` | 当前正文所含直接来源与显式版本依据的最新原始观察时间 |
| `memory_node_versions.content_updated_at REAL NULL` | 该正文版本的内容时间快照；明确再确认可更新同版本的时间 |
| `memory_derivation_sources.observed_at REAL NULL` | 直接来源观察时间的持久副本，支持删除原文后的拆分等操作 |
| `memory_nodes.updated_at` | 系统修改时钟，继续用于并发检查和内部调度 |

节点详情、节点列表及正文版本接口返回 `content_updated_at`，单位为UTC epoch秒。`null` 表示没有可用时间，`0` 是有效时间戳。长期记忆浏览与关联节点列表按内容时间降序、节点ID稳定排序，空值排在最后。

WebUI节点“更新时间”使用内容时间；历史版本区分“内容更新”和“版本生成”，来源派生记录显示操作时间。画像、人格关系、短期记忆的既有更新时间接口保持各自语义。

治理提交的 `expected_updated_at` 仍取节点的 `updated_at`，不能改用界面显示的内容时间。内容时间由存储层计算，无需在模型输出schema中增加字段，也不增加LLM调用。

旧库升级时由装配层注入 `SourceStore.get_observation_times` 供一次性复制可用原始观察时间。读取节点或版本不查询来源库；原文被清理后已有快照仍然有效。没有原文或精确版本凭据的记录不使用抽取时间补值。
