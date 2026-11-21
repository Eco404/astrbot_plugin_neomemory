# memory.db v6 与 ingress.db v1

2026-10-29。补充 [memory.db v5](memory-v5.md) 和 [ADR-0016](../../adr/0016-durable-ingress-and-context-completion.md)。source.db、cache.db 的schema保持v1。

## 原始待写库

`ingress.db` 独立于可清空缓存，`PRAGMA user_version=1`。唯一业务表为 `ingress_records`：

| 字段 | 职责 |
| --- | --- |
| arrival_no | AUTOINCREMENT接收顺序，不因删除队首而复用 |
| source_id / stream_id | 稳定来源标识、同流排序；source_id唯一 |
| payload_json / payload_hash / payload_bytes | 一份规范化输入及完整性摘要、UTF-8字节量；不是重复存储完整聊天历史 |
| state | queued / running / retry_wait / failed |
| attempts / total_attempts / max_attempts | 本轮重试次数、累计尝试和当前预算 |
| available_at / lease_token / lease_until | 退避与独占写权，旧worker不能ack新claim |
| error_code / created_at / updated_at | 安全诊断码与时间；不记录Provider异常正文 |

入队成功后才调用 `SourceStore.capture`。source提交成功再删除待写记录；两库之间无跨库原子提交，依靠source幂等和重复投递恢复。插件装配将两个数据库设为WAL、synchronous=FULL，避免先清待写而源提交仍只停留在未同步的WAL缓存。底层持久性仍依赖文件系统与硬件。

同流只claim最早未完成项，failed项会阻挡同流后续项；其他流可继续。租约到期可重领；正常取消释放claim，不消耗故障重试预算。合法JSON也要验证hash/字节和稳定ID，内容损坏进入failed而不写源库。

条数和字节容量在同一写事务检查，默认10000条/67108864字节，满时不淘汰。字节只计有效载荷，不承诺SQLite文件、空闲页或WAL小于64MiB。入队失败暂存本进程最多1000条应急记录，不覆盖已有项；超过时增加可见溢出计数。内存应急不是可靠持久队列，重启不能恢复。再次接收同流消息时，先补入更早的应急项。

成功后及时清除待写正文；队列没有成功历史表，不需要另做TTL清理。维护列表仅返回元信息，点击详情才读取正文；全部重试同时尝试重新接收内存应急项。`GET ingress`、`GET ingress/record`、`POST ingress/retry` 均使用宿主已认证API。

## 短期摘要增量

memory迁移6增加两张引用表，不改写既有节点、身份、画像或笔记：

| 表 | 内容和约束 |
| --- | --- |
| short_term_sources | state_id → short_term_states；source_id及当时sequence_no，主键(state_id,source_id)。精确表示摘要处理覆盖，而不是每条都贡献了事实 |
| short_term_receipts | job_id → memory_jobs；state_id、input_digest、result_json、committed_at。提交凭据与摘要更新同事务，防止重复生成/提交 |

继续使用 `short_term_states` 保存摘要正文、内容版本、状态、代、空间及coverage。conversation_id存 `host.v1:` 哈希键，输入为来源流和宿主conversation ID；coverage保留两者原标识与首次/末次sequence、observed_at、累计条数、最新证据ID、保留原文条数。不存在宿主conversation ID时不猜测摘要归属。

来源正文只保留在source.db，短期状态和检查点保存引用。跨数据库来源无SQLite外键，正式服务在生成和提交前重新查询当前空间/流/会话的精确有序前缀。首次非空摘要必须提供本批有效证据，继承已有非空摘要允许无新增证据；覆盖范围仍包含已判断可省略的来源。

模型请求在事务外；摘要、覆盖和receipt写入前复核任务租约、当前代、旧摘要版本与状态。已有receipt优先用于恢复。任务成功清除候选检查点；失败保留有界候选供审查，不放入cache。失败/取消只暂停同代同会话的短期窗口，显式重试成功后继续；长期窗口的顺序保障独立存在。

## 读取与升级边界

上下文快照及召回最终复核使用读事务 `BEGIN`，写事务继续 `BEGIN IMMEDIATE`。WAL外部writer不再仅因读取资料而使前台等待写锁。最终校验是一致快照，不是横跨宿主LLM调用的永久锁；返回后发生的修改在下一轮生效。

从旧memory库启动时自动迁移到v6。停用新功能可用独立开关；代码回退不自动降级schema。需要恢复旧版本时，先停止插件并做好一致文件备份，再使用匹配版本的整组数据库；不得直接删除未清空的ingress.db，也不能把关闭前的单个.db文件当作包含未合并WAL的完整备份。该说明不代表本轮实现了导入导出功能。
