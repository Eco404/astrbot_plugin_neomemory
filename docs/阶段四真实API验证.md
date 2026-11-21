# 阶段四真实 API 隔离验证

验证时间：2026-10-29 15:01–15:03（Asia/Shanghai）。

环境：Conda `astrbot`，Python 3.12，宿主源码 AstrBot v4.29.3。凭据来自本地 `docs/api_key.md`，本报告与脚本均不包含密钥、账户标识、角色卡、真实聊天或模型原文响应。

## 验证路径

- LLM：真实宿主 `ProviderOpenAIOfficial` → 真实 `Context.llm_generate` → 插件 `ProviderAdapter.generate`，模型 `gpt-6-luna`。
- Embedding：真实宿主 `OpenAIEmbeddingProvider` → `ProviderAdapter` → `EmbeddingService` → 独立临时 `CacheStore`，模型 `@cf/baai/bge-m3`。
- Cloudflare 使用 OpenAI-compatible `/ai/v1/embeddings` 路径；本次直连成功，未使用代理。
- Context 的 Provider 注册表由隔离脚本注入；没有启动完整 AstrBot 服务或消息平台。Provider 类、网络客户端、模型调用和 Context 的公开调用方法均为真实宿主实现。
- 运行根目录与 `cache.db` 位于临时目录，退出后删除；不写入正式 `source.db`、`memory.db` 或宿主配置。

## 结果

| 验证项 | 结果 | 观察值 |
| --- | --- | --- |
| Embedding batch 与重复文本复用 | 通过 | 4 条输入、3 条唯一文本，batch size=2、并发上限2；实际发出2次 HTTP 请求，返回4条1024维向量 |
| 维度学习与向量有效性 | 通过 | 宿主声明维度为0，缓存学习到1024维；所有向量有限、非零且归一化，重复输入获得同一结果 |
| 缓存关闭后重开 | 通过 | 0次 HTTP 请求，约0.0202秒；坐标最大误差约2.78×10⁻¹⁷，低于1×10⁻¹²容差 |
| 外层截止时间 | 通过 | 将真实宿主 Embedding 调用截止时间设为0.001秒，约0.0014秒返回 `TimeoutError`；本次请求发起计数1，没有插件额外重试 |
| 合成寒暄 extraction.v2 | 通过 | 一次真实生成调用，约11.3811秒；严格解析成功，`no_change`，节点/画像/覆盖变化均为空 |
| 合成事件进展 extraction.v2 | 通过 | 一次真实生成调用，约27.6846秒；严格解析成功，2个已有节点更新、1项 `full` 覆盖，未新增画像 |

Embedding 冷启动首轮耗时约2.9354秒；针对缓存比较修正后的第二轮约2.2146秒。上述时间只反映此次少量调用，不能当成稳定延迟或并发吞吐量指标。

第一轮缓存测试采用了不合适的逐位浮点相等判定：重开缓存后会再次归一化，产生极小的舍入差。网络调用数已为0；改为记录最大坐标误差并使用1×10⁻¹²容差后，仅重跑 Embedding 部分验证通过，没有重复调用 LLM。这是验证脚本的判定修正，无需修改缓存实现。

两轮合计2次 LLM HTTP 请求、6次 Embedding 请求；Embedding 中2次为有意触发截止时间的调用。不存在插件额外重试循环。

## 复现

脚本只输出模型名、计数、维度、耗时、schema 解析结果与错误类型等元信息；异常消息、原始响应、Provider 日志均不会输出。

```bash
PYTHONPATH=/workspace/astrbot/AstrBot:/workspace/astrbot/astrbot_plugin_neomemory \
  /home/coder/apps/miniconda3/envs/astrbot/bin/python \
  scripts/verify_stage4_api.py
```

仅重新验证 Embedding、缓存和截止时间：

```bash
PYTHONPATH=/workspace/astrbot/AstrBot:/workspace/astrbot/astrbot_plugin_neomemory \
  /home/coder/apps/miniconda3/envs/astrbot/bin/python \
  scripts/verify_stage4_api.py --skip-llm
```

需要时可以用 `--credentials` 指定本地凭据文档、用 `--embedding-proxy` 指定代理。脚本不修改 Provider 默认重试机制，所有网络操作受插件适配层的总截止时间约束。

## 实际覆盖范围与限制

本次确认了真实 API 可通过宿主 Provider 进入插件适配、批量向量与缓存管线，且两个合成样例能直接遵守 extraction.v2 严格 JSON 协议，无需修复输出。

截止时间测试验证的是插件对实际宿主 HTTP 操作的取消与返回边界，并非远端服务器慢请求、连接失败、限流重试的全面测试。1毫秒截止时间可能发生在连接或发送阶段，不能据此判断远端请求是否已经执行。

本次未执行正式记忆写入，也未验证模型抽取在真实长期对话、角色扮演、多个并行事件、复杂条件否定等情境下的质量。样例中的 `full` 覆盖仅证明模型能输出这一协议字段；全面正确率、稳定性和置信度仍需后续更大的标注回放集。阶段四的确定性状态校验、事务与召回正确性依靠离线测试分别验证。
