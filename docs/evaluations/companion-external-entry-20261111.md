# 陪伴外部记忆真实入口的独立合成验证

日期：2026-11-11。NeoMemory 提交 `1c07431038ee2886bced1dc9e03f0225d2b2dd39`，陪伴插件提交 `f180ff85a27172ffb696cb633f0465025cd567f2`。

当前结论：在原生 AstrBot 人格 ID 与陪伴公开角色 `subject_id` 本来一致的合法配置下，真实陪伴回复准备入口与 NeoMemory 实际外部适配器连接后的7个合成场景通过。两者不一致时，存在未解决的身份映射兼容问题。没有修改宿主、陪伴插件或 NeoMemory 生产实现；实机外部记忆接口继续关闭。

## 验证边界

场景和预期在执行前写入本地 manifest。使用陪伴插件现有 `GroupFixture`、实际 `AstrBotMultiController.prepare_core_reply`、公开 `UnifiedCompanionExtensionAPI` 和 Provider registry，连接 NeoMemory 的实际 `ExternalMemoryAdapter`、`SourceStore` 与 `MemoryStore`。数据库均在临时目录，节点由独立合成原始输入创建。

原生事件、平台和模型为陪伴插件既有测试替身；fixture 的 `NativeAdapterTypes` 声明版本为4.27.2，不是实机4.29.4完整运行。NeoMemory 召回服务返回固定合成节点，以验证身份解析、回调与上下文交付，不评估 Embedding、排序或模型语义。没有使用私人会话、真实角色卡或线上数据，没有执行实际回复生成、消息发送或实机配置写入。

## 身份一致的正向场景

在合成宿主启动前，将其合法人格映射配置为 `original → original`、`second → second`。NeoMemory 只按原生人格 `original` 建立一个空间，没有按回调结果追加或改名空间。

| 场景 | 实际适配器 read / 合成 recall 次数 | 结果 |
| --- | --- | --- |
| 原生空间读取 | 1 / 1 | 已保存节点进入 `prompt_instruction`，标记出现1次 |
| supplement | 1 / 1 | 标记出现1次，陪伴内置记忆仍可用 |
| 群聊 | 0 / 0 | 不调用私聊外部读取 |
| owner | 0 / 0 | 陪伴不调用外部 read，内置记忆归属让位 |
| 禁用后恢复 | 0 / 0 | 内置记忆恢复可用 |
| 合成超时 | 1 / 1 | 30ms截止取消阻塞的本地 recall 协程1次，回复准备成功，标记未注入，备用 provider 调用0次 |
| 读取期间注销 | 1 / 1 | 完成的旧回调结果被丢弃，回复准备成功，备用 provider 调用0次 |

总计 read 4次、合成 recall 4次、模型调用0次。7项均通过。

“标记1次”只覆盖陪伴的 `prepared.prompt_instruction` 范围，不包含 NeoMemory 自身 `on_llm_request` 与陪伴钩子的联合装配，不能据此宣称跨插件重复注入已解决。owner 场景只证明陪伴让位，未证明 NeoMemory 能覆盖所有 V2 原生回复路径。合成协程超时不代表真实 Provider、SDK重试或网络超时已验证。

## 身份不一致的兼容问题

保留原有合法合成映射 `original → role-a` 时，按原生人格创建的 NeoMemory 节点存在，但实际 read 只调用1次，召回服务调用0次，最终标记出现0次。回复准备本身成功，记忆缺失没有变成整个回复错误。

实际回调的 context 包含 `audience`、`principal`、`role`、`role_binding_revision`、`scope`、`user`。其中 role 只有 `installation_id`、`kind`、`subject_id`；scope 含平台、Bot、账户、会话等字段，没有原生 `persona_id`。该样本平台字段一致为 `qq-main`，缺口发生在人格映射。

[ADR-0021](../adr/0021-companion-external-memory-provider.md) 第3条要求使用结构化 context 中的人格、平台和认证账户映射到已有 NeoMemory 空间。当前 `adapters/external_memory.py::parse_external_scope` 将 `role.subject_id` 直接赋给 `persona_id`，`_private_space` 随后查找 `persona:role-a:...`，因此读不到以原生人格 `original` 建立的空间。不能以新增 `role-a` 空间或重命名旧空间来证明兼容。

在负例中，群聊排除、owner让位和禁用恢复3项通过，原生身份读取1项失败；依赖成功读取的补充模式、真实适配器超时和读取中注销3项记录为前置条件未满足，没有把它们伪报通过。原始失败结果与正向结果分别保存。

已检查陪伴 `UnifiedCompanionExtensionAPI` 的公开方法及外部记忆文档：记忆入口提供注册、注销、列表和关闭，没有接收当前 context 与绑定修订并返回已验证原生人格 ID 的公开映射方法。移动端身份接口也没有返回该字段。内部 `NativePrivateBridge.verified_persona` 能取得原生人格信息，但不属于上述公开接口；按照 ADR 第1条，NeoMemory 不能导入陪伴内部实现来绕过契约缺口。

最小修复方向需要公开契约补充：由陪伴一侧在本轮已核验的角色绑定下提供原生人格 ID，或提供接受完整 context/修订的公开映射能力，NeoMemory 再明确消费。该能力尚未实现或获准替代当前接口，报告不把“两个 ID 必须相等”写成新的产品要求。

影响范围：读取与统计共用 `_private_space`，均可能查错空间；外部写入也使用解析出的 `persona_id`，可能按公开角色 ID 保存来源。写入路由的具体后果尚未在联合探针中执行，不能只修改读取后宣称三种能力均已修复。缺少映射时也不能从昵称、正文或未绑定当前修订的配置中猜测身份。

## 现有验证与仍需完成的工作

- 陪伴既有 `tests.test_external_memory_providers`：17项通过，11.381秒。覆盖通用回调、真实回复准备入口、生命周期、预算和失败降级；其通用假 reader 不覆盖上述 NeoMemory 身份映射。
- NeoMemory 已有外部适配、热配置、短期、上下文和隔离宿主HTTP相关测试：73项通过，6.09秒。这是当前工程回归，不是新增真实平台或模型质量证明。
- 真实 Dashboard 已确认登录表单渲染、JWT鉴权后的完整 Vue 插件路由、五个桌面页签及390×844触控页签；首次提示弹窗正常关闭后可操作。密码提交、额外反代前缀、真实平台多人消息、完整写操作流程仍未覆盖。
- 当前全量重构结束后仍需执行浏览器 `--require-data`，检查新代节点、画像和关系的逐版本来源、直接原文及继承基线导航。空活动代不能用来验收这部分。
- 短期记忆可以脱离长期重构验证。已有独立真实模型材料覆盖两次滚动摘要及临时注入，但门限为保留1条/最低3条/最多4条。默认10/6/40门限持续触发、真实平台收发钩子、复杂指代/多人交错、称谓风格和精确宿主截断边界仍未验证；本次没有重复运行付费模型，也未把重构当作短期生成。
- 后续可在隔离实例增加默认门限的合成收发与后台任务验证，检查最新10条保留、重复前台请求不重复提交、宿主摘要标记抑制及临时历史过滤；该验证仍不能代替自然对话中的摘要语义与真实截断边界。

## 本地证据

材料位于不入Git的 `data/evaluations/live-rebuild-20261111/`：

- `companion-entry-manifest.json`、`companion-entry-result.json`：身份不一致负例；manifest SHA256 `f84a553bd619fe9b0bc520549c48af612d697f831bc8988ff2de966765bc8313`。
- `companion-entry-matched-manifest.json`、`companion-entry-matched-result.json`：身份本来一致的7项正向场景；manifest SHA256 `804f117db646b1579d575fe16751aa10a117e0bd1058bc5413d4983de9d3483e`。
- `companion_entry_probe.py`：独立探针，`--identity-mode mapped` 复现未解决兼容问题，`--identity-mode matched` 验证上述合法一致配置。
- `browser-click-analysis.md` 与 `browser-first-notice-touch-stable.json`：真实Vue点击遮挡归因与正常触控结果。

浏览器使用本地0600权限的短期JWT文件，认证刷新只读取远端配置、在远端内存签发，再捕获至本地私有文件；令牌不进入报告。SSH端口转发保留，终态检查前需重新确认令牌有效期。
