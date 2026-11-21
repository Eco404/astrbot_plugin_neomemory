# 可审阅宿主候选补丁

`astrbot-v4.29.3-no-context-truncation.patch` 基于 [AstrBot v4.29.3 / abf683071](https://github.com/AstrBotDevs/AstrBot/commit/abf683071)，也可应用于v4.29.4（`613354144`）。这是研究候选，**没有应用到现用宿主，NeoMemory自动抽取、重构和短期摘要尚未启用该能力**。适配器提供显式请求入口供接入使用。补丁中AstrBot派生部分按上游AGPL-3.0许可提供；上游版权和许可保持不变，参见[AstrBot LICENSE](https://github.com/AstrBotDevs/AstrBot/blob/abf683071/LICENSE)。

公开逐调用参数 `allow_context_truncation=False` 使OpenAI Chat和Responses在上下文超限回退删除消息之前抛出原异常；默认True保持行为。能力 `no_context_truncation.v1` 仅代表这一错误回退控制，不代表完整输入传输校验或HTTP次数硬上限。设计、receipt备选和未验证范围见[研究记录](../宿主输入完整性研究.md)。

官方最新稳定标签v4.29.4及主分支 `68acac91b` 尚无该能力；版本与独立复现见[上游核查](../evaluations/input-policy-adapter-20261111.md)。补丁通过测试不表示上游已采用。

本轮独立副本为 `data/references/astrbot-input-integrity/`，由指定提交的detached worktree创建，未改 `/workspace/astrbot/AstrBot` 工作树。补丁已通过基线上的 `git apply --check`。如需重新验证，在**独立副本**应用补丁后运行：

```bash
PYTHONPATH=/workspace/astrbot/astrbot_plugin_neomemory/data/references/astrbot-input-integrity:/workspace/astrbot/astrbot_plugin_neomemory \
  /home/coder/apps/miniconda3/envs/astrbot/bin/python -m pytest -q \
  tests/host_input_patch_contract.py
```

此测试文件不匹配默认 `test_*.py` 收集规则，避免未应用补丁的正式宿主测试失败。测试只使用合成数据及MockTransport，不读取凭据或调用真实API。现用宿主的缺陷复现仍在正常测试集 `tests/test_host_input_integrity.py` 中，它的通过表示已知行为被复现，不表示得到修复。

独立契约在v4.29.3和v4.29.4上均为 **24 passed**，包含适配器严格调用后的抽取提交与上下文失败不消费。v4.29.4副本位于 `data/references/astrbot-input-integrity-4.29.4/`，使用上述命令时替换PYTHONPATH首段即可。补丁 SHA-256：`0518e4f70333a43f4c7d7a0b1233164715be2578c9d5c55c9d81f81dd92cec40`。独立副本修改 `provider.py`、`sources/openai_source.py`、`sources/openai_responses_source.py`。Responses继承同一生成入口，只另行声明其具体类的能力；未知子类不自动继承能力声明。
