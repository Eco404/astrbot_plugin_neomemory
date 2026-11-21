# ADR-0022：宿主基础开关与 WebUI 配置即时生效

- 状态：Accepted
- 日期：2026-11-06
- 替代：ADR-0017 中保存后必须重载插件的配置生命周期；其余管理和任务契约继续有效

## 决策

1. AstrBot 插件配置表单仅显示 `enabled`、`capture_private_messages` 和
   `capture_group_messages`。其余字段保留在 `_conf_schema.json`，设置
   `invisible: true`；不从 schema 删除字段，避免宿主配置保存时丢弃高级设置。
   插件 WebUI 提供所有字段，设置 API 移除宿主表单使用的 invisible 标记。
2. WebUI 保存先通过宿主持久化接口写入配置，再调用当前插件实例的
   `apply_config`。成功响应要求保存值和运行值一致，`applied=true`，
   `reload_required=false`。应用不重新创建插件、关闭数据库或重启后台服务。
3. 正在执行的任务、召回和模型请求使用开始时取得的配置与依赖快照，后续工作使用
   新配置。调整并发限制影响后续许可发放；降低限制不取消已有请求。配置更新不
   取消已接收的持久任务，也不绕过任务恢复时原有的配置指纹与候选校验。
4. 配置保存和运行应用的结果分开表达。持久化成功而应用失败时返回
   `settings_apply_failed`，保留已保存内容，GET 返回 `applied=false` 和
   `apply_error`。读取设置不自动重试。用户可携带最新 revision 提交
   `changes={}` 重试应用已保存配置；没有待应用内容的空变更仍无效。
5. 前端保存成功即显示生效，不要求用户重载；失败时展示保存值与运行值的差异，
   提供“重试应用已保存配置”。此操作不提交尚未保存的页面草稿。
6. revision 检查和外部文件变更检测继续有效。宿主其他入口修改配置文件时，
   返回 `external_change=true`、`can_save=false`、`reload_required=true`，
   需要宿主重建配置对象。WebUI 内部保存即时应用不等同于监视和合并任意外部
   文件编辑，不承诺跨进程配置编辑的事务隔离。
7. 缓存 TTL 变更影响后续写入或续写，已存在缓存保留原到期时间；容量限额在
   后续缓存清理时执行，不在设置请求中扫描或重写全部缓存。关闭插件功能后暂停
   新工作的调度，已有工作继续完成；首次以关闭状态加载的实例在启用时初始化。

## 理由与边界

配置入口职责明确：宿主提供基础采集开关，插件 WebUI 集中展示模型、完整功能与
高级参数。保留完整宿主 schema 兼容既有保存机制和配置文件，不引入第二份配置库。

保存后立即更新后续工作的配置，避免用户为调整模型或门限而中断插件。任务快照
保证一次已开始的工作内部一致，并发许可保持全局计数；两者无需重启整个运行时。
已保存但未应用的状态必须可见、可重试，不能把写盘成功当作运行生效。

Embedding 模型或版本的改变继续遵循向量指纹和索引重建规则；切换配置不意味着
旧向量可以直接用于新模型。后台任务继续受现有作用域互斥、检查点和幂等提交约束。

## 验证要求

- 宿主表单只展示三个基础开关，WebUI 返回完整配置；隐藏字段经宿主保存仍保留。
- 保存后同一插件实例使用新配置，数据库实例持续存在；重启仍读取持久化后的值。
- 已开始的工作使用原配置完成，后续工作使用新值；并发限制降低时已有许可不中断。
- 持久化失败和应用失败分别验证，空变更重试使用最新 revision，GET 不产生应用副作用。
- 浏览器覆盖即时生效提示、失败重试、草稿保留、单项默认和外部配置变更提示。

## 实现依据

- AstrBot 宿主基线提交 `5c728936ceac0cce2ca341fbc3f5e1f03f6d4c78`
  （AGPL-3.0）的 [AstrBotConfig](https://github.com/AstrBotDevs/AstrBot/blob/5c728936ceac0cce2ca341fbc3f5e1f03f6d4c78/astrbot/core/config/astrbot_config.py)
  会清理 schema 外字段，`save_config_async` 本身不负责重载插件；其
  [配置组件](https://github.com/AstrBotDevs/AstrBot/blob/5c728936ceac0cce2ca341fbc3f5e1f03f6d4c78/dashboard/src/components/shared/AstrBotConfig.vue)
  支持 `invisible`。因此保留完整 schema 并使用宿主持久化接口，宿主版本变化时
  需要继续验证隐藏字段保留与保存行为。
- AnyIO 4.14.2（MIT）的
  [CapacityLimiter](https://github.com/agronholm/anyio/blob/4.14.2/src/anyio/_backends/_asyncio.py)
  将总容量与借出许可分开记录。NeoMemory 参考这一设计，以标准库
  `asyncio.Condition` 管理共享许可，不复制实现或新增依赖。模型调用均使用
  同一个计数器；缩容后的扩容仍按实际占用数判断可用容量，不依靠新旧限额的差值。
