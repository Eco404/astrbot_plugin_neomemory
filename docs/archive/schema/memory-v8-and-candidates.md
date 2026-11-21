# memory.db v8：失败候选证据

当前派生库版本为 8，在既有节点、画像、关系、任务和笔记表之外，增加 `memory_job_candidates`。打开库时自动迁移，原始库与缓存库版本不变。

| 字段 | 含义 |
| --- | --- |
| `candidate_id` | 任务、固定窗口和错误类别的确定性摘要，避免中断后重复归档 |
| `job_id` | 所属持久任务的外键引用 |
| `window_json` | 精确被拒绝候选及固定来源、上下文摘要、协议版本和诊断 |
| `error_code` | 不含原文的错误标识 |
| `created_at` | 归档时间 |

`POST tasks/retry` 保留原有 `job_id` 和 `fresh` 字段，并接受 `regenerate_window: true`；两个模式标志不能同时开启。该模式归档并清除当前输出，不丢弃已完成窗口、构建代、固定来源范围及上下文约束。排队和归档使用同一事务，仍遵守空间排他规则。

`checkpoint.window.regeneration` 保存 `{version: "candidate-regeneration.v2", candidate_id}`。下一次调用在固定输入后追加该归档中的完整候选和程序生成的结构化校验反馈，每次只引用紧邻的一份候选。读取归档同时检查所属任务与固定输入绑定；字段说明来自固定规则，不使用任意异常文本。`checkpoint.window.model_request_digest` 绑定包含反馈的完整请求，网络重试不得改写该请求。整体超出已有提示词预算时停止，不裁剪或静默修改内容。恢复协议不改变基础输入摘要、生产 schema 或执行签名，也不启动自动纠正循环。

任务列表返回 `regenerate_window_available`。任务详情同时返回 `validation_errors`、最近 10 份 `candidate_history` 以及 `candidate_history_has_more`。持久归档不随成功清除检查点而删除；它不是可清空向量缓存，也不作为新抽取的事实输入。

`validation_errors` 使用字段路径与稳定错误码，区分节点类型、预期正文版本、来源范围、用户证据归属、覆盖最终版本及关系约束。实际输入或数据库快照发生变化时仍属于上下文错误，不会伪装成可由模型修改字段解决的问题。诊断不包含原文或模型自行提供的异常文本。

`reference.v2` 用于模型输入输出传输，包含结构化标识符及派生正文中的 `{{user:…}}` 引用；数据库中的正式来源、节点与用户引用均为完整稳定 ID。协议版本参与输入摘要和执行签名。详细约束见 [ADR-0023](../../adr/0023-window-references-and-regeneration.md) 与 [ADR-0024](../../adr/0024-persona-voice-and-user-references.md)。展示字段不参与正文版本和治理提交。

JSON 语法错误的 v2 反馈从归档原文重新解析，提供字符偏移 pos（从0计数）、行列号 lineno/colno（从1计数）及固定语法说明，不转发原文片段或任意异常消息。仍支持已有 `candidate-regeneration.v1` 检查点，按其原有格式逐字重建反馈与请求，避免改变已持久化的请求摘要；新发起的重新生成使用 v2。
