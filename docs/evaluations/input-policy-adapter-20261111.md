# 抽取请求能力与上游版本核查

日期：2026-11-11。结论：已交付可显式调用的禁止截断适配器入口；官方最新稳定标签及最新主分支仍不能提供所需能力。生产抽取、重构与短期摘要尚未启用该入口，输入截断缺陷仍未解决。

## 上游证据

通过 `git ls-remote --tags --refs` 核对 AstrBot 官方仓库稳定标签，最高版本为 `v4.29.4`，提交 `61335414433db721b3f7ecc93f90deafe0a59d78`。默认分支为 `master`，核查时HEAD为 `68acac91b1ab92c2ac4cfd811c0898913a1391c8`。读取的是对应提交的独立checkout，没有切换或修改现用宿主。

两者的公开 `Provider.text_chat`、`Context.llm_generate` 和 OpenAI Chat/Responses 路径均未提供禁止上下文截断的接口。主分支的 [openai_source.py](https://github.com/AstrBotDevs/AstrBot/blob/68acac91b1ab92c2ac4cfd811c0898913a1391c8/astrbot/core/provider/sources/openai_source.py#L1112) 仍在context length错误后执行 `pop_record(context_query)`。Responses沿用对应生成入口。未将其他类型Provider判定为具有相同缺陷。

官方稳定版与主分支分别执行现有 `tests/test_host_input_integrity.py`，均为 **12 passed**。这些是缺陷及边界复现：包含删掉整个来源user消息后，模拟服务端返回合法no_change，正式事务仍消费来源；通过不表示修复。传输为MockTransport，不涉及真实模型或用户会话。主分支相比稳定版对最后一次重试成功判断的修改没有消除这条路径。

GitHub Releases API查询受未认证限流拒绝；版本结论以在线官方Git refs及源码为依据，不声称查询成功获得了Release元数据。插件最低版本仍为 `>=4.29.3`：当前没有可通过单纯提高版本要求获得此能力的正式版本。

## 可审阅适配器

`ProviderAdapter.supports_no_context_truncation(provider_id)` 读取公开 `get_request_capabilities()` 声明。`generate(..., require_no_context_truncation=True)` 在取得并发槽之后重新解析实际Provider，确认能力，再直接调用该实例公开的 `text_chat(contexts=..., allow_context_truncation=False)`。不具备能力时抛出 `InputPolicyUnsupportedError`，不发送模型请求。

绑定实际实例是必要的：原 `Context.llm_generate` 会异步按ID再次解析Provider。先检查一个实例，再调用另一个实例，不能构成能力保障。普通兼容调用仍走原Context路径；适配器不改变共享配置，不增加重试，不建立自己的SDK客户端。

能力仅表示context length错误处理不会删消息重发，不表示任意配置覆盖、正文改写或HTTP次数上限。此入口尚未从生产长期/短期任务调用，不能把它描述为整体修复。

## 验证

| 范围 | 结果 | 证明范围 |
| --- | --- | --- |
| 当前本地宿主 `5c728936c`，全量回归 | 1,318 passed，32.93秒 | 现有默认行为及适配器回归；包括已知缺陷复现用例 |
| 新适配器专项 | 10 passed | 能力发现、无能力零请求、绑定实际实例、排队热替换、取消释放槽、兼容调用不变 |
| 独立v4.29.3 + 候选补丁 | 24 passed，2.76秒 | Chat/Responses契约；含严格适配调用成功提交、超限不消费、参数不泄漏到上游请求 |
| 独立v4.29.4 + 同一候选补丁 | 24 passed，2.78秒 | 同上；补丁可直接应用，无须调整 |
| 未修改的官方v4.29.4 | 12 passed，15.84秒 | 输入缺陷仍可复现 |
| 未修改的官方master `68acac91b` | 12 passed，17.78秒 | 输入缺陷仍可复现 |

专项Ruff及diff检查通过。宿主自带audioop弃用告警不影响用例。新增成功提交测试首次使用了不符合正式schema的模拟输出，随后改为复用既有合法输出夹具；没有放宽解析器或省略事务验证。

全量验证使用 `/tmp` 中的隔离临时目录。现用宿主有其他开发改动，未触碰；所有参考checkout均位于被忽略的 `data/references/`。真实LLM、Embedding请求均为0；没有部署实机。

## 接入状态与剩余工作

用户确定暂不修改宿主，保留此问题，优先推进召回预算等独立优化。当前接口作为已验证的候选基础保留，不自动启用，也不提高最低宿主版本。

- 正式新版具备能力时，可按用户授权提高最低版本并继续接入；本次核查不满足该条件。若未来采用宿主补丁或纯插件有限缓解，需要重新明确范围。
- 将实际请求策略及能力纳入长期、重构与短期执行签名。已有raw候选会绕过模型调用，不能仅在generate中检查；已成功提交的原回执仍应优先恢复。
- 明确能力缺失的调度、维护状态及错误码，避免自动重复创建同类失败任务；连通性测试与能力状态分开展示。
- 验证完整后台管线的失败不消费、短期覆盖不推进、旧候选拒绝与显式恢复。当前独立契约不替代这些尚未接入的路径。

实施范围见[请求能力接入计划](../抽取请求能力接入计划.md)，宿主实现及许可见[候选补丁说明](../patches/README.md)。
