# Embedding 排队诊断与冷热缓存独立验证

## 范围与方法

验证对象是每次调用独立的 Embedding 诊断、缓存复用、历史补充查询及超时后的已有向量回退。未读取用户会话样本、原始人格或实机记忆；未改变生产截止时间、并发、批量大小或重试策略。

确定性测试使用独立文本与可控 Provider。真实请求使用本地 AstrBot 4.29.4 的 `OpenAIEmbeddingProvider`、`Context`，经过插件 `ProviderAdapter`、`EmbeddingService`、`RecallService` 和隔离 SQLite 存储；模型为 `@cf/baai/bge-m3`。真实探针不调用生成模型。

真实探针的节点、查询、历史文本与检查条件在网络请求前写入 manifest：三个手工构造的独立节点、一个当前问题、两条历史消息。先建立节点索引并缓存历史向量，再执行当前问题的冷查询及同一查询的热重复。历史中各有一条 user 和 agent 消息，均在默认历史时间范围内。

## 诊断合同

`RecallResult.diagnostics` 包含查询条数、事先缓存条数、可用向量条数、当前查询向量是否可用，以及查询缓存、Embedding 调用、整体召回耗时。

其中 `query_embedding` 是本次调用独立创建的字典，不通过全局最近一次指标传递：

| 字段 | 语义 |
| --- | --- |
| `service_queue_seconds` | 等待 EmbeddingService 复用锁的耗时，包含排队超时前已等待的时间 |
| `service_cache_seconds` | 获得锁后再次查缓存及校验缓存向量的耗时 |
| `cache_hit_count` | 获得锁后的缓存命中输入数；可能高于锁前命中数 |
| `planned_batch_count` | 本次调用调度的缺失向量批次数 |
| `provider_call_count` | 实际进入宿主 Provider 调用的批次数 |
| `provider_completed_batch_count` | 已完成宿主调用且响应校验通过的批次数 |
| `provider_queue_seconds_sum` | 各批等待 Provider 并发名额的时间之和 |
| `provider_call_seconds_sum` | 各批宿主 Provider 调用耗时之和 |
| `provider_failure_stages` | 失败或取消批次所在环节，区分并发排队、调用、响应校验 |
| `failure_stage`、`error_type` | 本次服务调用失败时所在环节和异常类型，不含异常正文 |
| `service_total_seconds` | 本次 EmbeddingService 调用总耗时 |

批次可以并发，因此两个 `_sum` 字段不是墙钟总耗时，不与 `service_total_seconds` 或召回 `total_seconds` 相加。宿主 Provider 调用时间可能包括其 SDK 内部行为，不能解释成纯网络传输时间。

`embedding_degraded=TimeoutError` 表示内部向量调用失败，已有缓存向量仍可能参与召回；`deadline_exceeded=true` 表示整个召回截止被触发。这两个指标分别判断，不能由其中一个推断另一个。

## 确定性结果

以下定向集合共 **110 项通过**：

```bash
/home/coder/apps/miniconda3/envs/astrbot/bin/python -m pytest -q \
  tests/test_providers.py tests/test_embeddings.py \
  tests/test_retrieval.py tests/test_extraction_context.py
```

针对诊断的新增或扩展检查包括：

- 服务复用锁等待超时：没有发起新的 Provider 请求，服务排队耗时可见。
- Provider 并发名额等待超时：排队耗时可见，调用耗时为零，没有发起请求。
- 已进入 Provider 的调用超时：实际调用计数为一，耗时记录完整，取消后释放名额。
- 两个批次、一个并发名额下超时：同时区分一个正在调用的批次和一个仍在排队的批次，不补发或重试。
- 冷调用去重并按批量切分；热调用完全复用；不同调用的诊断互不覆盖。
- 内部向量超时后，当前或历史缓存可以继续召回；真实服务与适配器组成的受控链路也验证了历史缓存回退。
- 忽略内部超时参数的替身被召回总截止取消，顶层截止与内部降级标识不同。

## 真实 Embedding 结果

本次共发出 **4 个 Embedding HTTP 请求、0 个生成模型请求**。其中节点索引两个请求、历史预热一个请求、冷当前查询一个请求。探针将 SDK 重试设为零，固定 HTTP 上限为八；生产重试行为未改变。

| 项目 | 冷当前查询＋两条已缓存历史 | 热重复查询＋两条已缓存历史 |
| --- | ---: | ---: |
| 查询向量条数 | 3 | 3 |
| 调用前缓存条数 | 2 | 3 |
| 最终可用向量条数 | 3 | 3 |
| 当前查询向量可用 | 是 | 是 |
| 本次 HTTP 请求 | 1 | 0 |
| 实际 Provider 调用批数 | 1 | 0 |
| Provider 调用耗时 | 0.678 秒 | 0 秒 |
| EmbeddingService 总耗时 | 0.684 秒 | 0.0020 秒 |
| 召回总耗时 | 0.691 秒 | 0.0092 秒 |
| 内部降级／总截止 | 均无 | 均无 |
| 返回节点／渲染体积 | 3／801 UTF-8 字节 | 3／801 UTF-8 字节 |

本次实际 Provider 排队耗时约 0.000007 秒，说明该隔离调用没有明显并发争用；不能据此推断实机繁忙时没有排队。

复现入口：

```bash
PYTHONPATH=/workspace/astrbot/AstrBot:/workspace/astrbot/astrbot_plugin_neomemory \
  /home/coder/apps/miniconda3/envs/astrbot/bin/python \
  -m scripts.probe_embedding_diagnostics --output data/evaluations/embedding-diagnostics-repeat
```

输出目录必须尚不存在。本次本地证据位于 `data/evaluations/embedding-diagnostics-20261116/manifest.json` 和 `report.json`，隔离数据库也在同一目录。凭据与数据库不进入 Git。

## 结论边界

已验证当前问题与有限历史可以复用向量，热重复无需联网；排队和实际调用时间可区分，内部失败时已有历史向量仍可发挥作用。

这里只执行一对冷热查询，不支持 P95、稳定吞吐、实机拥塞表现或检索准确率结论。三个返回节点没有事实相关性金标准，返回数量不作为质量分数。本次没有扩大默认截止时间，也未据此选择新的默认批量或并发参数。
