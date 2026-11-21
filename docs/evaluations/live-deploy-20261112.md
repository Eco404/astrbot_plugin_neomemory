# 实机更新、构建与召回验证

日期：2026-11-12。实机为 AstrBot v4.29.4，插件代码来自本地提交 `7e8e96d`。本次只更新插件运行文件，保留实机现有 `source.db`、`memory.db`、`cache.db`、`ingress.db` 和宿主配置，没有清理数据或启动全量重构。

## 构建与部署

本地运行 `scripts/build_plugin_package.py` 生成 `dist/astrbot_plugin_neomemory-v0.1.0.zip`，包含75个运行文件，包 SHA256 为 `82be23c99fa8e2ae394ca6596e68b5e6e398213d72588ff1d97eef509ea7694f`。远端上传包通过 ZIP 完整性检查，安装目录逐文件比较75/75一致。

插件目录采用暂存目录切换后重启 AstrBot。重启日志确认 NeoMemory v0.1.0 成功加载、注册 `neomemory_recall` 和 `neomemory_notes`，数据目录仍为 `/AstrBot/data/plugin_data/astrbot_plugin_neomemory`。切换产生的旧目录已在确认加载后删除。

## 实机状态

通过带宿主认证的插件 API 读取到：

- `ready=true`，AstrBot `4.29.4`；
- 原始来源 2237 条、1 个私聊空间；
- memory.db schema 10，节点 87 个，generation 2；
- cache.db schema 1，87 个节点向量，向量模型 1 个；
- ingress 待写数量为0，失败来源数量为0；
- 后台没有排队、运行或失败任务，已有重构任务为 succeeded；
- 实机配置仍选择 `ecoapi/gpt-6-luna` 与 `cloudflare_bge-m3`，生产默认提示词仍为 v7/v2。

健康状态中的 `scanner_error_type` 仍显示 `ProviderUnavailableError`，但 `last_error_type` 为空，模型列表能返回配置的 LLM 和 Embedding Provider，Embedding 探针任务也成功。该字段可能来自宿主启动早于 Provider 完成装配时的历史扫描错误，后续需要单独确认是否会影响新来源自动抽取；本次没有因此重构或修改配置。

## 召回回放

为避免修改实机数据库，先用 SQLite backup 将 source、memory 和 cache 三库复制到远端 `/tmp` 临时目录，再使用实机配置的真实 Cloudflare Embedding Provider 和生产 `RecallService`。回放不执行强化、不写回实机缓存，测试结束后删除临时文件。

回放使用3条当前节点正文自查询和4条预先固定的通用问句：

- 87个节点均有向量，7次 Embedding HTTP 请求全部返回 200；
- 4/4 条通用问句得到非空最终注入，均无超时、缺向量或降级；
- 3条自查询中2条最终包含查询节点；第1条查询节点进入候选第一位，但未进入最终注入，追加核查确认它不是向量缺失，而是最终候选选择阶段被其他节点取代；
- 自查询耗时约1.16–2.79秒，通用问句约1.06–2.50秒，均低于当前5秒召回截止；
- 实机插件自身的 Embedding 模型探针成功：维度1024，耗时约3.62秒。

这次使用私人样本只验证当前实现路径，不据此调整阈值、候选数量或提示词。自查询未进入最终注入的案例应与独立合成覆盖/预算测试结合分析，不能直接判定为普遍召回错误。

## 后续处理

1. 单独追踪 `scanner_error_type=ProviderUnavailableError` 的产生和自动扫描恢复，确认 Provider 装配时序不会阻塞新窗口抽取。
2. 使用独立合成数据复现“候选第一但最终未注入”的选择路径，区分覆盖替代、预算淘汰和状态重验证，不使用本次私人内容调参。
3. 在 v9 候选完成重复运行、多模型和来源充分性评估前，继续保持实机生产默认 v7/v2，不用 v9 重构现有记忆。
