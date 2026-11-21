# 抽取引用协议与有限纠正：实现参考

## 问题边界

模型需要在结构化候选中引用当前窗口的来源、已有节点和逻辑用户。数据库标识符是权威引用，模型输出只能选择已提供的标识符，不能补造来源，也不能通过模糊匹配将错误引用改成某个真实来源。这里讨论传输表示和失败恢复，不改变什么内容值得记忆、证据充分性或记忆覆盖的业务语义。

研究使用公开项目的本地源码快照，没有读取真实会话内容。下列实现证明设计存在，并不证明它们在 NeoMemory 上的质量或延迟收益；模型效果仍需独立合成输入及真实运行评估。

## 公开实现

| 项目与快照 | 许可证、维护证据 | 可借鉴内容 | 不适用的行为 |
| --- | --- | --- | --- |
| [Mem0](https://github.com/mem0ai/mem0/tree/26cdefb46600c0fa5953c4b5ee5003c125539fc4)，`26cdefb46600c0fa5953c4b5ee5003c125539fc4` | Apache-2.0；快照提交日期 2026-10-26 | [`mem0/memory/main.py:935`](https://github.com/mem0ai/mem0/blob/26cdefb46600c0fa5953c4b5ee5003c125539fc4/mem0/memory/main.py#L935) 将候选记忆 UUID 映射为顺序整数字符串，注释明确说明用于减少标识符幻觉。异步实现和 TypeScript 实现也使用短编号。 | 当前 additive 路径中构造了 `uuid_mapping`，但未发现回译使用，不能把这一段当作完整往返协议。其 JSON 宽松提取、解析失败转空结果等行为也不满足 NeoMemory 的失败可审阅契约。 |
| [Graphiti](https://github.com/getzep/graphiti/tree/5c27edfa8d3e0f49b2571bcebb4261bcc89ee037)，`5c27edfa8d3e0f49b2571bcebb4261bcc89ee037` | Apache-2.0；快照提交日期 2026-10-25 | [`node_operations.py:703`](https://github.com/getzep/graphiti/blob/5c27edfa8d3e0f49b2571bcebb4261bcc89ee037/graphiti_core/utils/maintenance/node_operations.py#L703) 生成 `candidate_id` 与 `candidates_by_id`，并在第 796–812 行将模型选择回译为真实节点和 UUID。来源引用使用 [`episode_indices`](https://github.com/getzep/graphiti/blob/5c27edfa8d3e0f49b2571bcebb4261bcc89ee037/graphiti_core/prompts/extract_nodes.py#L35)，再由程序映射到真实 episode。 | 非法候选 ID 被视为无重复；非法 episode 索引被丢弃，空列表回退成全部 episode。NeoMemory 必须拒绝这些情况，不能扩大证据范围。 |
| [Graphiti 的模型客户端](https://github.com/getzep/graphiti/blob/5c27edfa8d3e0f49b2571bcebb4261bcc89ee037/graphiti_core/llm_client/openai_base_client.py#L299) | 同上 | 应用错误携带错误类型和细节回传给模型，循环有上限；超时、连接和服务端错误交由 SDK 重试，避免把网络重试和内容纠正混成一个策略。 | 宽泛捕获应用异常并传 `str(e)` 不适合直接照搬；NeoMemory 应区分可纠正的模型输出与租约、版本、来源变更等上下文错误。 |
| [Hermes Agent](https://github.com/NousResearch/hermes-agent/tree/d05095663342556788cf2a198bfa7844cc7968ab)，`d05095663342556788cf2a198bfa7844cc7968ab` | MIT；快照提交日期 2026-10-25 | [`delegation_output_schema.py:87`](https://github.com/NousResearch/hermes-agent/blob/d05095663342556788cf2a198bfa7844cc7968ab/tools/delegation_output_schema.py#L87) 提供带字段路径的有界校验错误；[`delegate_tool_child_run.py:520`](https://github.com/NousResearch/hermes-agent/blob/d05095663342556788cf2a198bfa7844cc7968ab/tools/delegate_tool_child_run.py#L520) 最多执行一次纠正，再次失败显式保留无效结果。 | 不采用剥离包围文本、缺少校验库时直接接受 JSON 等宽松行为。Hermes 注释中关于多次纠正降低效果的判断属于项目经验，不能当作本插件已验证结论。 |

快照日期表示所检查本地版本的维护时点，不代表已核查当前上游最新状态。本方案借鉴公开设计，不复制第三方运行代码。

## 适用于 NeoMemory 的最小协议

1. 在固定的 `PreparedExtraction` 窗口上生成确定性的短引用，例如来源 `s1`、节点 `n1`、逻辑用户 `u1`。编号只在该窗口有效；真实来源 ID、顺序、版本和作用域仍保留在权威输入中。
2. 只对结构化标识符字段编码与解码，不对正文、人格文本或 JSON 字符串全局替换。模型自行声明的 `local_*` 引用保留原有语义。
3. 完整覆盖引用位置：所有 `source_ids`、更新节点的 `node_id`、边的两端、覆盖关系的两端，以及画像和人格关系中的 `logical_user_id`。输入中的用户身份也必须使用同一映射，避免同一个人在不同位置出现不同表示。
4. 按当前窗口字典精确回译，未知短引用、引用种类不符和越界编号直接拒绝；不接受前后缀猜测、编辑距离纠错或替换成全部来源。回译后继续执行现有 schema、来源、版本、作用域与事务校验。
5. 协议版本绑定输入摘要和任务检查点。已有候选使用其原协议，不将旧原始 ID 候选按新短引用协议解释。失败窗口重新准备时必须保留已完成工作和旧候选证据，并明确新候选对应的输入。

当前恢复机制是显式重新生成失败窗口的候选，保留已完成窗口和拒绝候选的审阅凭据。`candidate-regeneration.v3` 借鉴 Graphiti 的应用错误反馈及 Hermes 的字段路径：每次人工操作附加紧邻的一份被拒绝候选和固定规则生成的结构化反馈，并重新执行全部校验。JSON 语法错误采用与 Hermes 解析错误反馈类似的定位方式，但只提供数值行列号、字符偏移及固定语法说明。枚举、格式、数量及长度限制由声明schema派生，最多反馈12项。已有v1/v2检查点仍按原格式重建请求。反馈不直接转发异常文本，也不自动改写候选。来源、实际数据库版本、租约或输入上下文变化仍须停止；模型误填预期版本则由对应字段约束解释。不存在自动纠正循环，宿主网络重试与显式重新生成保持各自边界。

短引用会缩短重复出现的标识符，并减少逐字符复制负担，但不能消除语义归因错误。一个合法的 `s2` 仍可能指向错误证据；程序引用校验只能证明来源存在且在窗口内，不能独立证明其支持候选正文。

## 独立验证范围

引用模块的 20 个独立合成用例已通过，覆盖来源、节点、用户的完整往返；多个用户和空画像；不同字段复用同一引用；未知编号与错种类编号；合法 `local_*`；正文恰好出现 `s1` 时保持不变；由同一输入重新构造映射；支持的两种输出 schema。输入不从受限真实会话改写。

检查点、显式重新生成、游标和提交幂等性由管线集成测试验证，不能用引用模块测试代替。恢复测试覆盖无效事件时间反馈、完整候选保留、连续无效时不提交、每次仅一份旧候选、上下文与协议校验、请求预算和现有网络重试边界。独立合成真实 API 探针验证一次显式重新生成能够返回合法候选并提交；这一单例不能证明一般纠正成功率或语义质量提高。自动纠正策略仍未实现。

语义质量与协议通过率分开评估：引用正确不等于抽取正确。对模型效果的比较需固定独立输入、模型、窗口和输出检查标准，记录所有失败与延迟，不能只选通过的候选。
