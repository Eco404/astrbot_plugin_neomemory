# memory.db v9：逐版本正文与来源操作

v9 为节点、画像和人格关系提供可查阅的版本来源。决策与语义边界见 [ADR-0026](../../adr/0026-version-provenance.md)。原始正文保存在 source.db，本结构只存 ID 引用。

## 表结构

| 表 | 身份与内容 | 引用关系 |
| --- | --- | --- |
| `memory_node_versions` | 现有节点正文版本，不重复建立正文表 | `node_id` → `memory_nodes` |
| `profile_versions` | `profile_version_id`、`profile_id`、`version_no`、`tags_json`、`description`、`status`、`created_at` | `profile_id` → `user_profiles`；同画像版本唯一 |
| `relationship_versions` | `relationship_version_id`、`relationship_id`、`version_no`、`state_text`、`state_json`、`status`、`created_at` | `relationship_id` → `relationship_states`；同关系版本唯一 |
| `memory_derivations` | `derivation_id`、`operation`、`editor_kind`、`batch_id`、`governance_job_id`、`base_versions_json`、`created_at` | 三个可空目标版本外键中恰有一个：`node_version_id`、`profile_version_id`、`relationship_version_id`；另引用对应完成凭据 |
| `memory_derivation_sources` | `derivation_id`、`source_id`、`source_kind`；同操作来源 ID 唯一 | `derivation_id` → `memory_derivations`；source.db 为独立库，不设跨库外键 |

`base_versions_json` 是小规模版本引用列表，每项为 `{"kind":"node|profile|relationship","entity_id":"...","version_no":1}`。它只保存由已验证写入口确定的旧身份和版本，不复制正文。实体更新通常一个基线，人工合并可有多个基线。

画像/关系当前态与历史快照同事务更新。未变化的正文不新增快照，但合法的新引文可以新增操作。批次、正文和来源操作一起提交或回滚；完成凭据已经存在时直接返回，不产生重复记录。

## 查询接口

- `MemoryStore.list_node_versions(node_id)`：保留既有字段，按版本号升序返回。
- `MemoryStore.list_profile_versions(profile_id)`：返回版本快照，`tags_json` 解码为 `tags`。
- `MemoryStore.list_relationship_versions(relationship_id)`：返回版本快照，`state_json` 解码为 `state`。
- `MemoryStore.list_version_derivations(kind, entity_id, version_no=None)`：查询实体的全部或指定版本操作，`kind` 为 `node`、`profile`、`relationship`；不存在实体抛 `KeyError`。

三类版本均包含 `provenance_status` 与 `derivations`。`provenance_status=recorded` 表示有操作记录；`unrecorded` 表示没有逐次来源记录，不能等同于没有事实来源。

派生操作字段为：

```json
{
  "derivation_id": "derivation_...",
  "version_no": 2,
  "operation": "update",
  "editor_kind": "model",
  "batch_id": "batch_...",
  "governance_job_id": null,
  "base_versions": [{"kind": "profile", "entity_id": "profile_...", "version_no": 1}],
  "sources": [{"source_id": "source_...", "source_kind": "source_record"}],
  "created_at": 0.0
}
```

`operation` 为 `create`、`update`、`confirm`、`edit`、`split`、`merge`；`editor_kind` 沿用 `system`、`model`、`manual`、`rebuild`。重构仍通过自动抽取生成候选，其操作编辑者为 `model`，重构归属通过批次的 generation 和任务凭据确定。

没有来源参数的公共画像/关系写入口记录为 `system` 且直接引文为空。人工编辑或合并通过基线追溯原内容，不能把基线来源显示成本次直接引文。累计 `node_sources` 继续用于原有节点来源查询，与历史引文分别展示。

## 迁移语义

旧画像或关系只复制现存当前正文，版本号原样保留，例如原计数为 4 时只有第 4 版快照，不创造第 1–3 版。迁移不生成派生记录，不把整个批次、时间邻近或节点累计来源当作确定引文。已有节点版本、作用域和来源累计表不变。

新写入口产生的版本可以以迁移保留的当前版本为基线；因此新链条可追溯至一个明确标为未记录历史引文的旧版本。原文清理不删除引文 ID，也不影响正文版本的读取。
