# `memory.db` 基础 schema 与兼容迁移

`memory.db` 保存可重建的派生数据。它与 `source.db` 分离，任何删除或重构操作都不能删除原始来源。

## 表关系

```text
memory_spaces ──< memory_generations ──< memory_nodes ──< memory_node_versions
                   │                      ├─< node_sources
                   │                      ├─< node_participants
                   │                      ├─< memory_edges（两个节点端点）
                   │                      └─< node_coverage（事件与原子）
                   ├─< user_profiles
                   ├─< relationship_states
                   └─< short_term_states
memory_spaces ──< notes（独立于 generation）
```

节点当前正文通过 `memory_nodes.content_version` 连接到 `memory_node_versions.version_no`。`node_sources.source_id` 是 `source.db.source_records.source_id` 的逻辑引用，不能使用跨库 SQLite 外键。

## 状态和版本约束

- 节点正文变化创建新版本；来源、参与者、关系变化不创建正文版本。
- 节点删除采用 `deleted` 状态，来源记录仍在 `source.db`。
- 节点状态只允许 `active / historical / disabled / superseded / deleted`。
- `manual_protected` 是独立布尔字段，表示自动流程不得直接覆盖正文，可与 `active`、`historical`、`disabled` 同时存在。修改状态不会解除保护；使用 `set_manual_protection(node_id, False)` 显式解除。
- `manual_protection_reason` 保存可选保护原因；解除保护时清空原因。人工修改仍可产生新的正文版本；版本历史通过 `list_node_versions(node_id)` 查询。
- 每个空间最多一个 `memory_generations.status = 'active'`。
- 节点关系端点必须在同一个空间和 generation。
- 重构生成新 generation，按原始输入和当前规则重新创建节点，不复制旧节点的保护标记、人工修改和拆合结果。

## v1 到 v2 的人工保护迁移

v1 已有保护字段，但曾把 `manual_protected` 同时列为状态。v2 将旧的 `status = manual_protected` 转换为 `status = active, manual_protected = 1`，新增可选原因字段，保留全部节点 ID、版本、来源、关系和 coverage 引用。其他状态和已有的保护标记保持不变。

为避免重建节点表影响外键，旧表的 CHECK 定义仍保留旧枚举文本；v2 使用 INSERT/UPDATE 触发器拒绝把 `manual_protected` 再写为状态，直接 SQL 也不能绕过。重新打开数据库不重复迁移。

迁移由 `storage/memory_migrations.py` 按版本号管理；当前目标版本以其中的 `CURRENT_SCHEMA_VERSION` 为准。本文记录基础表及保护迁移，抽取凭据和强度扩展见 [v3 schema](memory-v3.md)。

## 运行时装配

插件先打开 `source.db`，再创建 `MemoryStore(source_exists=SourceStore.has_source)` 并打开 `memory.db`。真实原始来源不存在时，派生节点的来源写入被拒绝。该回调不承担空间权限判定，抽取应用层仍需验证来源空间、人格和批次范围。

健康接口保留原始库状态，并在 `memory` 字段中报告派生库的 schema 版本、节点数和空间数。初始化失败或被取消时释放已打开的数据库；正常退出按 `memory.db → source.db` 的顺序关闭。
