# memory.db v7 与阶段六管理 API

日期：2026-10-29。本文补充 [memory.db v5](memory-v5.md) 和 [memory.db v6 / ingress.db v1](memory-v6-and-ingress-v1.md)。以当前代码为准；本阶段只把 `memory.db` 升至 v7，`source.db`、`cache.db`、`ingress.db` 版本不变。

实现入口：[console_api.py](../../../adapters/console_api.py)、[console_store.py](../../../storage/console_store.py)、[settings_api.py](../../../adapters/settings_api.py)、[settings.py](../../../core/settings.py)、[model_tests.py](../../../core/model_tests.py)。

## 1. memory v7：笔记提交凭据

[迁移 7](../../../storage/memory_migrations.py) 追加 [note_receipts](../../../storage/console_schema.py)，不改写既有笔记正文和身份：

| 字段 | 类型与约束 | 含义 |
| --- | --- | --- |
| `job_id` | TEXT PRIMARY KEY，引用 `memory_jobs(job_id)` | 一次持久笔记任务最多一个提交凭据 |
| `space_id` | TEXT NOT NULL，引用 `memory_spaces(space_id)`，ON DELETE CASCADE | 笔记所属空间 |
| `note_id` | TEXT NOT NULL，引用 `notes(note_id)` | 本次创建或修改的笔记 |
| `input_digest` | TEXT NOT NULL | 规范化操作 JSON 的 SHA-256 摘要 |
| `result_json` | TEXT NOT NULL | `note_id`、`space_id`、`action`、`status`、`updated_at`；不重复笔记正文 |
| `committed_at` | REAL NOT NULL | 提交时间，Unix 秒 |

笔记继续使用已有 `notes` 表。**笔记归属空间，不归属派生代**：私聊空间仍为人格 × 逻辑用户，群聊空间为人格 × 平台实例 × 群聊。重构切代保留笔记及其提交凭据；笔记操作不要求 `expected_generation_id`，不因派生代改变而自动冲突。

WebUI 写笔记创建 `kind=note`、`scope_key=space_id` 的持久任务。其 `payload.operation` 保存规范化命令，成功任务也保留该命令以支持幂等比对和人工审阅；正文不是复制进 receipt。命令和 receipt 均不属于可清空缓存。

执行时在同一个写事务内检查 running 状态、lease token、租约期限、任务类型和载荷，再检查 receipt。已有相同 receipt 时返回原结果及 `replayed=true`；否则验证笔记归属和修订条件，更新笔记并写 receipt，然后由任务执行器完成任务。若进程在正文提交后、任务 finish 前退出，恢复后不会再次创建或覆盖笔记。

更新、归档、恢复使用 `expected_updated_at` 做乐观并发检查。调用者须原样回传 GET 中的数值，不截断或格式化为日期字符串。写入使用 `max(now, nextafter(previous_updated_at, +∞))`，同一时刻或时钟回退时也保证修订标识严格增加。提交前发现旧页面返回 409；提交后、执行前出现冲突则任务失败，错误码为 `note_revision_changed`，不会静默覆盖。

空间互斥沿用 [memory v5](memory-v5.md)：queued/running/retry_wait 排斥同空间任务，`*` 任务排斥所有作用域。Agent 工具仍即时调用笔记 CRUD，但其写事务内也检查活动任务，不能绕过 WebUI 笔记任务、整理或重构的锁。取消先撤销租约；取消不能撤销此前已提交的正文。笔记断点重试保留原版本条件；`fresh=true` 被拒绝，版本冲突应重新读取笔记后提交新的编辑。

默认人格 `default`、`_chatui_default_`、`[%None]` 不允许创建新笔记任务；遗留默认人格笔记任务返回跳过结果。旧数据库内容不会被因此自动删除。升级自动执行，代码回退不自动降级 schema。

## 2. 路由与共同约定

以下路径均由宿主 `register_web_api` 注册，插件前缀为 `/astrbot_plugin_neomemory`。WebUI 使用 `AstrBotPluginView.apiGet/apiPost`，由桥接层处理宿主外层路由及认证；不另启无鉴权 HTTP 服务。

成功响应直接是下文所列 JSON 对象，没有统一的 `data` 外壳。错误使用宿主格式：

```json
{"status":"error","message":"公开错误说明","data":{"error_code":"console_conflict"}}
```

错误正文不包含 Provider 的原始异常、URL 或密钥。原文、候选和笔记正文仅从明确的查阅接口返回，任务列表只返回摘要。

共同分页参数：`limit` 默认 50，范围 1–100；`offset` 默认 0，范围 0–1,000,000。URL 中的十进制整数字符串可解析，布尔值、负数和小数不是合法分页值。`q` 默认空，最多 1,000 字符；使用 SQLite `instr(lower(...), lower(?))` 作字面匹配，`%`、`_` 没有通配语义。这里不是全文检索，也不承诺完整 Unicode 大小写折叠。

响应分页结构：

```json
{
  "space_id":"space_...",
  "generation_id":"gen_...",
  "items":[],
  "limit":50,
  "offset":0,
  "has_more":false
}
```

查询在 SQL 中过滤后再分页；`has_more` 通过多读取一条计算。每次读取内部一致，但多次翻页不是冻结游标；其间更新或切代可能改变排序。稳定空间 ID 与 `scope_key` 不是可互换参数：这些空间查阅和笔记命令要求真实 `space_id`。

## 3. 记忆查阅

### `GET memory/context`

必填 `space_id`、`kind`。可选分页参数、`q`、`status`，其中 `status` 默认 `all`。

| kind | 可选 status | items 的专属字段 |
| --- | --- | --- |
| `profiles` | all / active / disabled / historical | `profile_id`、`logical_user_id`、`description`、`tags` 数组 |
| `relationships` | all / active / disabled / historical | `relationship_id`、`logical_user_id`、`state_text`、`state` 对象 |
| `short_term` | all / active / disabled / archived | `state_id`、`conversation_id`、`summary_text`、`coverage` 对象、起止来源引用 |

每项同时包括空间、代、状态、`content_version`、创建/更新时间。`tags_json`、`state_json`、`coverage_json` 解码后分别作为 `tags`、`state`、`coverage` 返回，不要求前端二次解析 JSON 字符串。响应额外带回 `kind`。

当前代查询和内容读取在同一读事务中，避免返回新代 ID 与旧代正文的混合结果。按 `updated_at DESC`、记录 ID 排序。画像搜索包括描述和序列化标签，关系/短期搜索对应正文。群聊不提供人格关系，返回空列表；本接口不修改画像、关系或短期摘要。

### `GET memory/nodes` 的新增筛选

沿用原路由、分页结构、节点字段和默认“当前代全部状态”行为，新增：

- `node_type=all|atomic|event|topic`，默认 all。
- `status=all|active|historical|disabled|superseded|deleted`，默认 all。
- `q`：在当前正文版本上作字面筛选。

同一读事务选择当前代和节点。正文通过版本表引用读取；排序为 `updated_at DESC,node_id`。这是管理查询，不受向量召回的 2,000 节点快照上限截断。既有 `GET memory/versions?node_id=...` 继续返回当前代节点、该条目当前保留的版本（最新在前，默认含当前版本在内最多 10 个）、来源、参与者和关系；退休代节点版本查阅返回维护冲突，不能据此修改新代。

### `GET memory/source`

必填 `space_id`、`source_id`。返回：

```json
{"space_id":"space_...","source":{"source_id":"source_...","content_text":"...","metadata":{}}}
```

`source` 同时包含原记录和来源流的身份、角色、时间、序号、消息标识等字段；`metadata_json` 解码成 `metadata`。不返回 `generation_id`，因为原始内容独立于派生代。

不接受仅凭任意来源 ID 直接查看。按记录采集时的人格和逻辑用户归属验证私聊空间；Agent 回复采用触发用户/同流前序用户的原有归属规则。群聊同时验证人格、平台实例、群 ID。采用与来源扫描相同的身份语义，后续账户绑定不会在此静默改写旧记录归属。

尚未成功抽取的来源也可查看，供失败窗口审阅；不要求当前节点已经引用该来源。退休代曾引用的原文依然可查看，读取时发生重构切代也不影响相同原始空间的归属。来源不存在/已清理、身份未解析、跨空间、默认人格或当前尚未支持的 `source_event` 返回 404。此接口只读，不实现原始库清理。

## 4. 笔记 API

### `GET notes`

必填 `space_id`，支持共同分页和 `q`。`status=active|archived|all`，默认 active；all 包括 active 与 archived，不包含 deleted。

返回共同分页结构，项目包括 `note_id`、`space_id`、`persona_id`、`logical_user_id`、`content_text`、`status`、`source_kind`、`source_ref`、解码的 `metadata`、创建/更新时间等。其中顶层 `generation_id` 只是读取当时的空间元信息，**不是笔记归属或修改条件**。按 `updated_at DESC,note_id` 排序。

### `POST notes`

每个请求必须含 `space_id`、`idempotency_key`（非空、最多 200 字符）、`action`。UTF-8 JSON 请求上限 100,000 字节。不接受未列出的字段。

| action | 另外必填字段 | 行为 |
| --- | --- | --- |
| update | `note_id`、`expected_updated_at`、`content_text` | 修改正文，保留既有 active/archived 状态 |
| archive | `note_id`、`expected_updated_at` | 仅 active → archived |
| restore | `note_id`、`expected_updated_at` | 仅 archived → active |
| delete | `note_id`、`expected_updated_at` | 将条目标记为 deleted |

正文须为非空字符串，修剪首尾空白后最多 20,000 字符；这是管理 API 上限，Agent 工具仍使用自身更小的输入限制。新笔记由 Agent 通过 `neomemory_notes` 创建。

示例：

```json
{
  "space_id":"space_...",
  "idempotency_key":"web-unique-operation",
  "action":"update",
  "note_id":"note_...",
  "expected_updated_at":1793260000.123456,
  "content_text":"修订后的完整笔记"
}
```

返回**直接的任务摘要**：`job_id`、`kind=note`、`scope_key`、`state`、`step`、进度、尝试次数、安全错误码和时间。提交成功只代表任务持久排队，不代表正文已改完。后续通过 `GET task?job_id=...` 或任务列表查询状态。

同一幂等键、同空间、同规范化操作返回原任务，包括已完成任务；相同键但载荷/空间/任务类型不同返回 409。重复提交已完成的更新不会因为原 `expected_updated_at` 过期而再执行或新建任务。幂等键使用已有任务库的全局唯一约束，调用者应为不同意图生成新键。

任务详情的 `candidate` 可包含经过校验的笔记操作及正文，供管理员审阅；成功结果为 receipt 中的标识、状态、更新时间及 `replayed`。版本冲突、对象不可用分别以 `note_revision_changed`、`note_unavailable` 记录。任务重试/取消继续使用既有 `tasks/retry`、`tasks/cancel`。

## 5. 设置读取、保存和默认值

### `GET settings`

返回：

| 字段 | 含义 |
| --- | --- |
| `schema` | 完整 `_conf_schema.json` 的字段定义、类型、默认值、说明和范围；额外提供 group，移除仅用于宿主表单的 invisible |
| `values` | 当前宿主配置对象中的已保存值，缺失字段补 schema 默认值 |
| `runtime_values` | 运行中的 `NeoMemoryConfig` 值 |
| `revision` | 宿主完整配置快照摘要，保存时原样提交 |
| `can_save` | 宿主具备保存能力、插件未关闭且没有检测到外部文件变更 |
| `applied` | 已保存值已成功应用，且与当前运行值一致 |
| `apply_error` | null，或最近一次应用失败时的 settings_apply_failed |
| `reload_required` | 发现外部配置文件变化，需要宿主重建配置对象 |
| `external_change` | 配置文件与本服务记录的基线不同 |

`group` 为 `model`、`功能`、`高级`：三个 Provider ID 归 model，布尔项归功能，其余归高级；前端可把并发等模型相关参数安排到模型页。字段默认和上下限以返回 schema 为准，不在此重复维护另一份参数清单。

AstrBot 插件表单显示模型选择和功能设置。高级参数保留在宿主 schema 中并使用 `invisible: true` 隐藏。WebUI 返回完整 schema 并移除 invisible 标记，可编辑全部参数。插件启停由 AstrBot 插件管理控制。

只公开插件已知字段；宿主未知字段不返回，但参与 revision 并在保存时保留。服务优先调用宿主异步保存接口，否则在线程中调用同步保存接口，不直接替宿主重写配置文件。

### `POST settings`

请求恰好为 `{"revision":"64字符摘要","changes":{"字段名":值}}`。`changes` 只包含当前 schema 字段；请求 JSON 不超过 100,000 字节。通常提交非空变更；保存值尚未应用或上次应用失败时，允许 `changes={}` 重试应用当前保存值。没有待应用内容的空变更返回 400。

验证严格使用 JSON 类型和范围，不把字符串数字/布尔值悄悄转换。私聊及群聊门限须满足 min ≤ topic ≤ max，短期门限须满足 min ≤ max。无效组合返回 400，不自动排序或压缩到边界。Provider ID 允许保存为当前不可用的字符串，实际能力通过模型列表和连通测试确认。

单项“恢复默认”没有特殊后端命令：前端把 `schema[field].default` 写入该字段草稿，再按普通 changes 保存；点击恢复默认仅修改草稿，保存后应用到运行实例。

保存先持久化宿主配置，再调用插件的 `apply_config`。成功返回与 GET 相同的结构，`values == runtime_values`、`applied=true`、`reload_required=false`，**无需重载插件**。应用不关闭数据库或重启插件；已开始的任务继续使用配置快照，后续工作使用新配置。并发上限调整影响后续许可，不取消已在执行的请求。短期专用 `short_term_provider_id` 为空时回退长期抽取的 `llm_provider_id`。浏览器关闭不撤销已开始的保存和应用；插件关闭等待已接收操作结束。

缓存 TTL 调整用于后续写入或续写，已有缓存保持原到期时间；容量限额由后续缓存清理执行。后续功能调用读取已应用的配置。

持久化成功但应用失败时，返回 500 / `settings_apply_failed`，已保存值保留。GET 是只读操作，返回新 revision、保存值、当前运行值、`applied=false` 和 `apply_error=settings_apply_failed`；该状态无需重载。前端读取新 revision 后，可提交空 changes 重试应用，或提交新的有效变更；重试应用不提交页面中未保存的草稿。

revision 防止本插件多个页面静默覆盖，文件摘要额外发现来自其他页面/进程的已发生变更。检测到外部文件变化时 can_save=false，需在宿主重载插件，单纯刷新 WebUI 不会重建宿主配置对象。它不能给其他进程加锁，不能承诺消除任意外部写入的全部时间竞态。

## 6. 模型列表与持久连通测试

### `GET models`

沿用现有接口，返回 `{"llm":[...],"embedding":[...]}`。每项包含 `provider_id`、`model`、`provider_class`；Embedding 额外包含 `dimension`。列表直接来自 AstrBot 已加载 Provider，不返回连接地址、密钥或完整 Provider 配置；这不是远端供应商的模型目录发现。宿主列表读取失败返回 503。

### `POST models/probe` 与 `POST models/test`

两个入口均直接测试指定的宿主 Provider。请求示例为 `{"kind":"embedding","provider_id":"configured-provider"}`。可选类型为 `llm` 和 `embedding`。测试使用合成输入，与记忆管线共享 Provider 并发限额和调用超时。

成功响应包含 `ok`、`kind`、`provider_id`、`model` 和 `elapsed_ms`。向量测试额外返回实际维度 `dimension`。失败响应包含错误类别。连通性结果仅在模型页展示，测试不会写入任务库。

## 7. 状态码与兼容注意

| 接口族 | 情形 | HTTP / error_code |
| --- | --- | --- |
| context/source/notes | 未就绪或关闭中 | 503 / console_unavailable |
| context/source/notes | 空间/对象/原文不可用 | 404 / console_not_found |
| context/source/notes | 活动任务或版本/幂等冲突 | 409 / console_conflict |
| context/source/notes | 参数无效 | 400 / invalid_console_request |
| context/source/notes | 其他内部失败 | 500 / console_request_failed |
| settings / probe | revision、外部修改或测试互斥冲突 | 409 / settings_or_model_test_conflict |
| settings | 宿主保存能力不可用 | 503 / settings_unavailable |
| settings | 保存失败 | 500 / settings_save_failed |
| settings | 配置已保存，但运行应用失败 | 500 / settings_apply_failed |
| settings / probe | 请求字段、类型或范围无效 | 400 / invalid_settings_or_model_request |
| probe | 运行服务未就绪 | 503 / model_test_unavailable |
| settings / probe | 其他内部失败 | 500 / settings_or_model_request_failed |

`memory/nodes` 继续使用既有 maintenance 错误码。`POST tasks` 的可提交 kind 列表没有扩展为任意任务：note 必须走 POST notes，model_test 必须走模型测试入口，从而保留各自校验。通用任务查询、进度、重试和取消可处理新增任务类型。

读取响应应允许新增字段；写入请求按上述明确字段发送，不直接回传完整 GET 对象。界面切换空间或切换节点时应使旧请求失效，不能把旧响应渲染进新上下文。来源、笔记和模型文本均作为数据展示，不解释为 HTML。

本轮不包含导入导出、原始库自动清理、账户跨平台合并操作、画像/关系/短期摘要的任意手工编辑或大规模向量索引替换。规模边界另见[阶段六规模评估](../../阶段六规模评估.md)。

## 后台任务删除与生成诊断

`POST tasks/delete` 接收 `{"job_id":"…"}`。非 running 任务可删除；运行中返回冲突，需先取消。删除清除任务、检查点和拒绝候选，以及该任务专属未启用重构代与向量。已生效记忆、原始记录和笔记保留，尚未消费来源允许重新被自动调度。

任务详情增加 `automatic_corrections`（当前窗口已使用自动纠错次数）及 `response_diagnostics`（宿主可提供的响应状态、结束原因、用量、耗时和是否重复候选）。格式错误不再单独标为 `restart_required`；优先当前窗口重新生成，保留既有进度。
