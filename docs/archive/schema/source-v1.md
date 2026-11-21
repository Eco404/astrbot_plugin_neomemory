# source.db v1

阶段一的 `source.db` 只保存原始输入和身份快照，不保存 LLM 抽取的原子、事件、主题或 Embedding。

## 事实来源

- `source_records.content_text` 是阶段一的规范化文本。
- `source_records.content_json` 是可选的消息组件 JSON，供审查和后续重构使用。
- `source_records.observed_at` 是插件观察时间；事件发生时间只有在来源明确提供时才保存。
- `source_records.actor_logical_user_id` 是写入时身份解析快照，账户绑定变更不会静默改写历史记录。
- `source_records.persona_id` 是写入时人格解析快照；无法可靠解析时为 `NULL`，同时 `identity_pending=1`，不使用猜测的人格名称补齐。
- `conversation_streams.persona_id` 保存该流最近一次可解析的人格，便于查看；单条记录的 `persona_id` 才是重放时使用的事实快照。

## 幂等性

`(stream_id, dedup_key)` 唯一。优先使用平台或 LLM 响应 ID；没有稳定外部 ID 时使用输入流、角色、内容和观察时间生成回退键。重复提交返回已有 `source_id` 和 `sequence_no`，不会推进游标。

## 迁移

迁移版本保存在 `schema_migrations`。新版本只追加迁移，不修改旧迁移文本。测试数据库必须从空文件执行全部迁移，生产数据库升级后记录版本和时间。
