# ADR-0021：接入陪伴插件外部记忆 Provider

- 状态：Accepted（适配契约）；真实陪伴回复链路仍需宿主联调
- 日期：2026-11-06
- 相关资料：[陪伴插件外部记忆接口](/workspace/astrbot/astrbot_plugin_private_companion/docs/external-memory-provider.md)

## 背景

`astrbot_plugin_private_companion` 提供公开的
`companion.external-memory.v1` 接口，可由第三方插件注册 `read`、`write` 和
`summary` 回调。NeoMemory 已经拥有稳定的原始库、派生库和召回服务，需要明确
两套插件之间的身份映射、失败边界和数据归属，避免导入陪伴插件私有实现或重复创建
一套 Provider 重试逻辑。

## 决策

1. 只通过 `context.get_all_stars()` 和公开的 `extension_api` 发现并注册 Provider，
   不导入陪伴插件的内部模块。
2. Provider 默认关闭。启用后支持两种显式模式：
   - `supplement`：注册为补充记忆，`handles_model_memory=False`；
   - `owner`：声明 NeoMemory 负责模型侧记忆，`handles_model_memory=True`。
   两种模式都必须由部署配置明确启用，不能根据插件是否安装自动切换。
3. 身份只接受陪伴接口传入的结构化 `context`。私聊读取使用
   `persona_id + platform + authenticated native account` 映射到 NeoMemory 的
   `persona_id + logical_user_id` 空间；群聊不会调用此私聊读取接口。
4. `read` 只返回已经保存的完整记忆节点，使用现有有界召回服务，不额外调用生成模型，
   不从查询文本推断用户或人格。
5. 带平台送达回执的主动消息以人格的会话发言写入原始库。每个送达分段带有稳定身份、平台会话、发送人格、收件人及送达时间，可参与近期上下文、抽取和重构。旧接口的整条主动联系记录、日记和梦境作为 `source_event` 留存。
6. 写入按来源插件和记忆 ID 保持幂等。相同 ID但内容、人格、安装、平台、账户、会话或
   群作用域冲突时拒绝，不静默覆盖。外部记录按来源插件和宿主作用域进入稳定的原始流，
   记忆 ID只承担去重。回调内部不增加网络重试，异常交由宿主按接口规定降级；进入持久
   ingress 前严格要求标签、元数据和可选字段可 JSON 序列化。
7. `summary` 只返回有限计数，不返回正文、凭据或调试数据。注销、停用和 Provider
   超时由宿主处理，NeoMemory 不改投其他 Provider。

## 后果

- NeoMemory 可以作为陪伴插件的可选外部记忆来源，并为未来 Agent 自主活动保留原始输入入口。
- `source_event` 在专门的活动抽取协议建立前不会出现在长期记忆召回中，避免把未定义的自主活动语义当作会话事实。
- `supplement` 与 NeoMemory 自身 AstrBot 请求注入的重复关系，需要在真实宿主链路中继续验证；本 ADR 不宣称已经完成重复注入消除。
- `owner` 模式只表达 Provider 归属，不替代对陪伴插件自有 V2 回复路径的端到端验证。

## 回滚与验证

- 关闭 `external_memory_enabled` 或卸载 Provider 即可停止注册，不迁移、不删除现有数据库记录。
- 适配层测试覆盖身份拒绝、Provider 生命周期、私聊读取、`source_event` 写入、冲突和统计；
  真实陪伴插件加载、浏览器链路、模型效果和跨插件重复注入另列宿主联调任务。
