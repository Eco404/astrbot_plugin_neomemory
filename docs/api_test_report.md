# 真实 API 连通性测试报告

> 测试日期：2026-10-29
> 
> Python 环境：Conda `astrbot`（Python 3.12.13）
> 
> 说明：本报告不记录 API token，也不保存模型返回正文。

## 测试对象

| 类型 | 配置 | 测试内容 |
| --- | --- | --- |
| LLM | ecoapi / `gpt-6-luna` | `/models`、最小聊天请求、Lumi/Yui 角色卡请求 |
| Embedding | Cloudflare / `@cf/baai/bge-m3` | 单文本向量、3 条文本 batch 向量 |

## 结果

### LLM

- `/models`：HTTP 200，约 0.02 秒，返回正常 JSON。
- 最小 `/chat/completions`：HTTP 200，约 2.60 秒，返回 `choices` 和 `usage`，内容非空。
- Lumi 角色卡：HTTP 200，约 12.67 秒，模型返回非空内容。
- Yui 角色卡：HTTP 200，约 10.93 秒，模型返回非空内容。

### Embedding

- 单文本请求：HTTP 200，约 0.95 秒，返回 1 个向量，维度 1024。
- 3 条文本 batch 请求：HTTP 200，约 2.69 秒，返回 3 个向量，每个维度 1024。

## 结论

当前两个真实 API 均可用，能够支持后续阶段的初步 LLM 抽取测试和 Embedding batch/缓存测试。Embedding batch 请求已经验证可用，后续可以在插件 Provider 适配层使用 batch size 参数。

本次测试只验证了网络、认证、基本请求格式和返回结构，没有验证 AstrBot Provider 适配、长上下文、限流、重试、超时、结构化 JSON 遵循率或记忆抽取质量。正式进入阶段三前，应使用隔离的回放样本继续测试这些内容。
